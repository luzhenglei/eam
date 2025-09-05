# blueprints/connect.py
from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from services.project_service import get_project
from services.device_service import search_devices_in_project, get_device
from services.link_service import find_candidates, create_link, delete_link, list_links_in_project

bp_connect = Blueprint("connect_bp", __name__, url_prefix="/projects")

@bp_connect.route("/<int:pid>/connect", methods=["GET"])
def connect_page(pid):
    p = get_project(pid)
    if not p:
        flash("项目不存在", "err")
        return redirect(url_for("projects_bp.project_list"))
    return render_template("connect_config.html", project=p)

# --- AJAX: 搜索设备（当前项目内，按编号/型号模糊） ---
@bp_connect.route("/<int:pid>/api/search-devices")
def api_search_devices(pid):
    q = request.args.get("q", "")
    rows = search_devices_in_project(pid, q)
    return jsonify({"ok": True, "data": rows})

# --- AJAX: 候选端口（两台设备） ---
@bp_connect.route("/<int:pid>/api/candidates")
def api_candidates(pid):
    a_id = request.args.get("a", type=int)
    b_id = request.args.get("b", type=int)
    if not a_id or not b_id:
        return jsonify({"ok": False, "err": "参数缺失"})
    try:
        # 校验设备属于项目
        a = get_device(a_id); b = get_device(b_id)
        if not a or not b or a.get("project_id") != pid or b.get("project_id") != pid:
            return jsonify({"ok": False, "err": "设备不在该项目"})
        c = find_candidates(pid, a_id, b_id)
        return jsonify({"ok": True, "data": c})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)})

# --- AJAX: 建立连接 ---
@bp_connect.route("/<int:pid>/api/link", methods=["POST"])
def api_make_link(pid):
    a_port = request.form.get("a_port_id", type=int)
    b_port = request.form.get("b_port_id", type=int)
    if not a_port or not b_port:
        return jsonify({"ok": False, "err": "参数缺失"})
    try:
        lid = create_link(pid, a_port, b_port, status='CONNECTED')
        return jsonify({"ok": True, "id": lid})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)})

# --- AJAX: 删除连接 ---
@bp_connect.route("/<int:pid>/api/link/<int:link_id>/delete", methods=["POST"])
def api_delete_link(pid, link_id):
    try:
        delete_link(pid, link_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "err": str(e)})

# --- AJAX: 列出现有连接（用于着色与断开） ---
@bp_connect.route("/<int:pid>/api/links")
def api_list_links(pid):
    rows = list_links_in_project(pid)
    return jsonify({"ok": True, "data": rows})
