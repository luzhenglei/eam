from flask import Blueprint, render_template, request, redirect, url_for, flash
from services.template_service import list_templates, list_template_attributes, upsert_template_attributes

bp_templates = Blueprint("templates_bp", __name__, url_prefix="/templates")

@bp_templates.route("/bind", methods=["GET","POST"])
def bind():
    templates = list_templates()
    tpl_id = request.values.get("template_id", type=int)
    scope = request.values.get("scope", default="device")

    rows = []
    if tpl_id:
        rows = list_template_attributes(tpl_id, scope=scope)

    if request.method == "POST" and tpl_id:
        include_ids = request.form.getlist("include_ids")
        required_ids = request.form.getlist("required_ids")
        upsert_template_attributes(tpl_id, include_ids, required_ids, scope=scope)

        flash("模板属性已保存", "ok")
        return redirect(url_for("templates_bp.bind", template_id=tpl_id, scope=scope))

    return render_template("bind.html", templates=templates, rows=rows, tpl_id=tpl_id, scope=scope)
