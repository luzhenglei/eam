from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from services.device_service import (
    list_devices,
    get_device,
    get_template_attrs_for_form,
    save_device_attributes,
    create_device_basic,  # 用于新增设备
    delete_device,
    update_device_basic,  # 用于更新设备
    _ensure_ports_for_device,  #
    get_device_preview_data,
    create_child_port,
)
from services.template_service import list_templates
from services.option_service import list_children

bp_devices = Blueprint("devices_bp", __name__, url_prefix="/devices")


@bp_devices.route("/", methods=["GET"])
def list_page():
    return redirect(url_for("projects_bp.project_list"))

@bp_devices.route("/<int:device_id>/preview", methods=["GET"])
def preview_page(device_id):
    try:
        data = get_device_preview_data(device_id)
    except Exception as e:
        flash(f"预览失败：{e}", "err")
        return redirect(url_for("devices_bp.list_page"))
    return render_template("device_preview.html", data=data)

@bp_devices.route("/new", methods=["GET", "POST"])
def new_device():
    templates = list_templates()
    if request.method == "POST":
        template_id = request.form.get("template_id", type=int)
        name = (request.form.get("name") or "").strip()
        model_code = (request.form.get("model_code") or "").strip()
        if not template_id or not name or not model_code:
            flash("模板、设备名称、型号必须填写", "error")
            return render_template("device_new.html", templates=templates)

        try:
            dev_id = create_device_basic(template_id, name, model_code)
            flash("设备已创建，请填写属性后保存。", "success")
            return redirect(url_for("devices_bp.edit_attrs", device_id=dev_id))
        except Exception as e:
            flash(f"创建失败：{e}", "error")
            return render_template("device_new.html", templates=templates)

    return render_template("device_new.html", templates=templates)

@bp_devices.route("/<int:device_id>/edit", methods=["GET", "POST"])
def edit_device(device_id):
    """
    编辑设备基本信息（名称、型号、模板）
    GET：渲染编辑表单（自己已有 UI 的话可复用；这里不提供模板代码）
    POST：提交更新；若切模板，联动清理并重建端口实例
    """
    device = get_device(device_id)
    if not device:
        flash("设备不存在", "error")
        return redirect(url_for("devices_bp.list_page"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        model_code = (request.form.get("model_code") or "").strip()
        new_template_id = request.form.get("template_id", type=int)
        if not name or not model_code:
            flash("名称与型号必填", "error")
            return redirect(url_for("devices_bp.edit_device", device_id=device_id))

        try:
            update_device_basic(device_id, name, model_code, new_template_id)
            flash("设备已更新", "success")
            return redirect(url_for("devices_bp.list_page"))
        except Exception as e:
            flash(f"更新失败：{e}", "error")
            return redirect(url_for("devices_bp.edit_device", device_id=device_id))

    # GET：需要模板列表供选择
    templates = list_templates()
    return render_template("device_edit.html", device=device, templates=templates)


@bp_devices.route("/<int:device_id>/delete", methods=["POST"])
def delete_device_route(device_id):
    """
    删除设备（POST 提交）
    """
    try:
        delete_device(device_id)
        flash("设备已删除", "success")
    except Exception as e:
        flash(f"删除失败：{e}", "error")
    return redirect(url_for("devices_bp.list_page"))

@bp_devices.route("/<int:device_id>/attrs", methods=["GET", "POST"])
def edit_attrs(device_id):
    device = get_device(device_id)
    if not device:
        flash("设备不存在", "err")
        return redirect(url_for("projects_bp.project_list"))

    template_id = device.get("template_id")
    if not template_id:
        flash("该设备未绑定模板，无法编辑属性", "err")
        return redirect(url_for("projects_bp.project_detail", pid=device.get("project_id")))

    # 每次进入做一次端口对账/补齐
    try:
        _ensure_ports_for_device(template_id, device_id)
    except Exception as e:
        flash(f"端口同步失败：{e}", "err")

    if request.method == "POST":
        form_model = get_template_attrs_for_form(template_id, device_id)
        ok, msg = save_device_attributes(device_id, form_model, request.form)
        if ok:
            flash("已保存属性", "ok")
            # 关键：跳回该设备所在项目
            return redirect(url_for("projects_bp.project_detail", pid=device.get("project_id")))
        else:
            flash(msg or "保存失败", "err")
            return render_template("device_attrs_form.html", device=device, attrs=form_model)

    form_model = get_template_attrs_for_form(template_id, device_id)
    return render_template("device_attrs_form.html", device=device, attrs=form_model)

# --------- 端口相关 API ---------

# @bp_devices.route("/<int:device_id>/ports/<int:parent_port_id>/children", methods=["POST"])
# def create_child_port_api(device_id, parent_port_id):
#     name = (request.json.get("name") if request.is_json else request.form.get("name")) or ""
#     name = name.strip()
#     if not name:
#         return jsonify({"ok": False, "msg": "name required"}), 400
#     try:
#         cid = create_child_port(device_id, parent_port_id, name)
#         return jsonify({"ok": True, "data": {"id": cid}})
#     except Exception as e:
#         return jsonify({"ok": False, "msg": str(e)})

# --------- AJAX API：按父子关系返回直接子项 ---------
@bp_devices.route("/options-children")
def api_options_children():
    attribute_id = request.args.get("attribute_id", type=int)
    parent_id = request.args.get("parent_id", default=None, type=int)
    if not attribute_id:
        return jsonify({"ok": False, "msg": "attribute_id required", "data": []})
    try:
        rows = list_children(attribute_id, parent_id)
        data = [{"id": r["id"], "name": r["name"]} for r in rows]
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e), "data": []})


@bp_devices.post("/devices/<int:device_id>/ports/<int:parent_port_id>/children")
def create_child_port_api(device_id: int, parent_port_id: int):
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "msg": "name required"}), 400
    try:
        new_id = create_child_port(device_id, parent_port_id, name)  # ✅ 不再限制层级
        return jsonify({"ok": True, "id": int(new_id)})
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 400