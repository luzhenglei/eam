# blueprints/port_types.py
from flask import Blueprint, render_template, request, redirect, url_for, flash
from services.port_type_service import (
    list_port_types, get_port_type, create_port_type, update_port_type, delete_port_type
)

bp_port_types = Blueprint("port_types_bp", __name__, url_prefix="/port-types")

@bp_port_types.route("/", methods=["GET"])
def list_page():
    rows = list_port_types()
    return render_template("port_type_list.html", rows=rows)

@bp_port_types.route("/new", methods=["GET", "POST"])
def new_page():
    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        name = (request.form.get("name") or "").strip()
        try:
            create_port_type(code, name)
            flash("已新增端口类型", "ok")
            return redirect(url_for("port_types_bp.list_page"))
        except Exception as e:
            flash(f"新增失败：{e}", "err")
    return render_template("port_type_form.html")

@bp_port_types.route("/<int:pt_id>/edit", methods=["GET", "POST"])
def edit_page(pt_id):
    item = get_port_type(pt_id)
    if not item:
        flash("端口类型不存在", "err")
        return redirect(url_for("port_types_bp.list_page"))
    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        name = (request.form.get("name") or "").strip()
        try:
            update_port_type(pt_id, code, name)
            flash("已保存修改", "ok")
            return redirect(url_for("port_types_bp.list_page"))
        except Exception as e:
            flash(f"保存失败：{e}", "err")
    return render_template("port_type_form.html", item=item)

@bp_port_types.route("/<int:pt_id>/delete", methods=["POST"])
def remove(pt_id):
    try:
        delete_port_type(pt_id)
        flash("已删除端口类型", "ok")
    except Exception as e:
        # 常见原因：被 port 或 port_template 引用
        flash(f"删除失败：{e}", "err")
    return redirect(url_for("port_types_bp.list_page"))
