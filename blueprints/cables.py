# blueprints/cables.py
from typing import Any, Dict, List
from io import BytesIO
import csv

from flask import Blueprint, render_template, request, send_file, jsonify, flash, redirect, url_for

from services.project_service import get_project
from services.link_service import (
    list_cables_paginated,
    fetch_all_cables,
    fetch_cables_by_ids,
    mark_links_printed,
)

bp_cables = Blueprint("cables_bp", __name__, url_prefix="/projects")


def _make_labels(project_name: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    为导出/打印补全标签与组合列。
    不再依赖端口方向，直接：
      FROM/TO = A_LABEL / B_LABEL
      TO/FROM = B_LABEL / A_LABEL
    其中  A_LABEL = <项目>-<设备A>-<端口A>
         B_LABEL = <项目>-<设备B>-<端口B>
    """
    out: List[Dict[str, Any]] = []
    for r in rows:
        a_label = f"{project_name}-{r['a_device_name']}-{r['a_port_name']}"
        b_label = f"{project_name}-{r['b_device_name']}-{r['b_port_name']}"
        r2 = dict(r)
        r2.update({
            "a_label": a_label,
            "b_label": b_label,
            "from_to": f"{a_label} / {b_label}",
            "to_from": f"{b_label} / {a_label}",
        })
        out.append(r2)
    return out


@bp_cables.route("/<int:pid>/cables", methods=["GET"])
def cables_page(pid: int):
    p = get_project(pid)
    if not p:
        flash("项目不存在", "err")
        return redirect(url_for("projects_bp.project_list"))

    page = request.args.get("page", type=int, default=1)
    page_size = request.args.get("page_size", type=int, default=50)
    data = list_cables_paginated(pid, page, page_size)
    items_raw = data["items"]

    items = _make_labels(
        p["name"],
        [
            {
                "a_device_name": r["a_device_name"],
                "a_port_name": r["a_port_name"],
                "a_port_type_name": r["a_port_type_name"],
                "b_device_name": r["b_device_name"],
                "b_port_name": r["b_port_name"],
                "b_port_type_name": r["b_port_type_name"],
                "a_dir": r["a_dir"],
                "b_dir": r["b_dir"],
                "link_id": r["link_id"],
                "printed": r["printed"],
                "printed_at": r["printed_at"],
            }
            for r in items_raw
        ],
    )

    return render_template(
        "cables_list.html",
        project=p,
        items=items,
        page=data["page"],
        page_size=data["page_size"],
        total=data["total"],
    )


@bp_cables.route("/<int:pid>/cables/export", methods=["POST", "GET"])
def cables_export(pid: int):
    """导出：all=1 导出全部；否则用 ids[]=... 导出选中"""
    p = get_project(pid)
    if not p:
        flash("项目不存在", "err")
        return redirect(url_for("projects_bp.project_list"))

    ids_form: List[str] = request.form.getlist("ids")
    ids_query_raw = request.args.get("ids", "")
    ids_query: List[str] = [x for x in ids_query_raw.split(",") if x.strip()]
    ids: List[int] = [int(x) for x in (ids_form or ids_query) if str(x).strip().isdigit()]

    rows = fetch_all_cables(pid) if (request.values.get("all") == "1" or not ids) else fetch_cables_by_ids(pid, ids)
    rows = _make_labels(p["name"], rows)

    # 优先导出 XLSX；若导入失败则回退 CSV
    try:
        try:
            from openpyxl import Workbook  # 仅当环境有依赖时走 xlsx
        except ImportError:
            raise

        wb = Workbook()
        ws = wb.active
        ws.title = "Cables"
        headers = [
            "A_PROJECT",
            "A_DEVICE",
            "PORT_TYPE",
            "A_PORT",
            "A_LABEL",
            "FROM/TO",
            "B_PROJECT",
            "B_DEVICE",
            "PORT_TYPE",
            "B_PORT",
            "B_LABEL",
            "TO/FROM",
            "PRINTED",
            "LINK_ID",
        ]
        ws.append(headers)
        for r in rows:
            ws.append(
                [
                    p["name"],
                    r["a_device_name"],
                    r.get("a_port_type_name") or "",
                    r["a_port_name"],
                    r["a_label"],
                    r["from_to"],
                    p["name"],
                    r["b_device_name"],
                    r.get("b_port_type_name") or "",
                    r["b_port_name"],
                    r["b_label"],
                    r["to_from"],
                    "YES" if r.get("printed") else "NO",
                    r["link_id"],
                ]
            )
        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)
        return send_file(
            bio,
            as_attachment=True,
            download_name=f"{p['name']}_cables.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except ImportError:
        # CSV 回退（你若已决定只保留 xlsx，也可以删除这一分支）
        bio = BytesIO()
        writer = csv.writer(bio)
        writer.writerow(
            [
                "A_PROJECT",
                "A_DEVICE",
                "PORT_TYPE",
                "A_PORT",
                "A_LABEL",
                "FROM/TO",
                "B_PROJECT",
                "B_DEVICE",
                "PORT_TYPE",
                "B_PORT",
                "B_LABEL",
                "TO/FROM",
                "PRINTED",
                "LINK_ID",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    p["name"],
                    r["a_device_name"],
                    r.get("a_port_type_name") or "",
                    r["a_port_name"],
                    r["a_label"],
                    r["from_to"],
                    p["name"],
                    r["b_device_name"],
                    r.get("b_port_type_name") or "",
                    r["b_port_name"],
                    r["b_label"],
                    r["to_from"],
                    "YES" if r.get("printed") else "NO",
                    r["link_id"],
                ]
            )
        bio.seek(0)
        return send_file(
            bio,
            as_attachment=True,
            download_name=f"{p['name']}_cables.csv",
            mimetype="text/csv; charset=utf-8",
        )


@bp_cables.route("/<int:pid>/cables/printed", methods=["POST"])
def cables_mark_printed(pid: int):
    """批量标记为已打印"""
    ids_str: List[str] = request.form.getlist("ids")
    ids: List[int] = [int(x) for x in ids_str if str(x).strip().isdigit()]
    cnt = mark_links_printed(pid, ids)
    return jsonify({"ok": True, "count": cnt})


@bp_cables.route("/<int:pid>/cables/print", methods=["GET"])
def cables_print(pid: int):
    """打印预览：仅显示选中 ids 的记录"""
    p = get_project(pid)
    if not p:
        flash("项目不存在", "err")
        return redirect(url_for("projects_bp.project_list"))

    ids_raw = request.args.get("ids", "").strip()
    link_ids: List[int] = [int(x) for x in ids_raw.split(",") if x.isdigit()]
    rows = fetch_cables_by_ids(pid, link_ids)
    rows = _make_labels(p["name"], rows)
    return render_template("cables_print.html", project=p, items=rows)
