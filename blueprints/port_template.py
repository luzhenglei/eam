from flask import Blueprint, render_template, request, redirect, url_for, flash
from services.template_service import list_templates  
from services.port_type_service import list_port_types  
from services.device_service import (
    list_port_templates,
    create_port_template,
    delete_port_template,
)

tpl_bp = Blueprint("tpl_bp", __name__, url_prefix="/templates")


@tpl_bp.route("/")
def templates_home():
    # 简单列出模板（已有页面也行）
    templates = list_templates()
    return render_template("templates_list.html", templates=templates)


@tpl_bp.route("/<int:template_id>/port-templates", methods=["GET", "POST"])
def port_tpl_manage(template_id):
    if request.method == "POST":
        code = (request.form.get("code") or "").strip()
        name = (request.form.get("name") or "").strip()
        qty = request.form.get("qty", type=int) or 1
        naming_rule = (request.form.get("naming_rule") or "").strip() or None
        sort_order = request.form.get("sort_order", type=int) or 0
        port_type_id = request.form.get("port_type_id", type=int) or None  # 新增
        max_links = request.form.get("max_links", type=int) or 1

        if not code or (qty <= 1 and not name and not naming_rule):
            flash("请至少填写 code；若 qty=1，需要填写 name 或 naming_rule", "error")
        else:
            try:
                create_port_template(template_id, code, name, qty, naming_rule, sort_order, port_type_id, max_links)  # 传入
                flash("已新增端口规则", "success")
            except Exception as e:
                flash(f"新增失败：{e}", "error")

        return redirect(url_for("tpl_bp.port_tpl_manage", template_id=template_id))

    rows = list_port_templates(template_id)
    port_types = list_port_types()  # 供下拉选择

    # 构建预览树：端口类型 -> 属性 -> [端口名...]
    ports_tree = {}
    for r in rows:
        ptype = r.get("port_type_name") or "未分类"
        attr = r.get("name") or "（未命名属性）"
        code = (r.get("code") or "").strip()
        qty = int(r.get("qty") or 1)
        if qty < 1:
            continue
        names = [f"{code}{i}" for i in range(1, qty + 1)]
        ports_tree.setdefault(ptype, {}).setdefault(attr, []).extend(names)

    return render_template(
        "port_template_manage.html",
        template_id=template_id,
        rows=rows,
        port_types=port_types,
        ports_tree=ports_tree,
    )


@tpl_bp.route("/port-templates/<int:pt_id>/delete", methods=["POST"])
def port_tpl_delete(pt_id):
    try:
        delete_port_template(pt_id)
        flash("已删除端口规则", "success")
    except Exception as e:
        flash(f"删除失败：{e}", "error")
    # 简单返回模板主页；也可以从表单带回 template_id
    return redirect(url_for("tpl_bp.templates_home"))
