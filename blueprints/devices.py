from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from services.device_service import (
    list_devices,
    get_device,
    get_template_attrs_for_form,
    save_device_attributes,
    create_device_basic,  # 用于新增设备
)
from services.template_service import list_templates
from services.option_service import list_children

bp_devices = Blueprint("devices_bp", __name__, url_prefix="/devices")


@bp_devices.route("/")
def list_page():
    rows = list_devices()
    return render_template("devices_list.html", rows=rows)


@bp_devices.route("/new", methods=["GET", "POST"])
def new_device():
    """
    补回“新增设备”路由，避免页面中 url_for('devices_bp.new_device') 报 BuildError。
    """
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


@bp_devices.route("/<int:device_id>/attrs", methods=["GET", "POST"])
def edit_attrs(device_id):
    device = get_device(device_id)
    if not device:
        flash("设备不存在", "error")
        return redirect(url_for("devices_bp.list_page"))

    from services.device_service import _ensure_ports_for_device
    _ensure_ports_for_device(device["template_id"], device_id)

    # 再取页面模型（这里会读 port 表，得到端口区块）
    form_model = get_template_attrs_for_form(device["template_id"], device_id)

    if request.method == "POST":
        # 1) 基于 request.form 构造一个“兼容两种风格”的 payload
        #    - 保留 attr_{aid}（我们新版 save_device_attributes 使用）
        #    - 同时构造 payload[aid]["enum_option_ids"]（旧代码路径使用）
        payload = {}

        # 先把原始 form 的所有键值原封不动放进去（保留 chain、root/text_i、普通输入框等）
        for k in request.form.keys():
            vals = request.form.getlist(k)
            if len(vals) == 1:
                payload[k] = vals[0]
            else:
                payload[k] = vals  # 多选下拉会是 list

        # 兼容层：为 flat_attrs 的枚举型，补出 payload[aid]["enum_option_ids"]
        for item in form_model.get("flat_attrs", []):
            aid = item["attribute_id"]
            dtype = item["data_type"]
            allow_multi = bool(item["allow_multi"])
            key = f"attr_{aid}"

            if dtype == "enum":
                raw = request.form.getlist(key) if allow_multi else [request.form.get(key)]
                ids = []
                for v in raw:
                    if v and str(v).strip().isdigit():
                        ids.append(int(v))
                # 去重
                ids = list(dict.fromkeys(ids))
                # 写入兼容结构：payload[aid] = {"enum_option_ids":[...]}
                payload[aid] = {"enum_option_ids": ids}
            else:
                # 非枚举：也补一个 {"value_text": "..."} 以防旧代码读取
                v = (request.form.get(key) or "").strip()
                payload[aid] = {"value_text": v}

        # 2) 保存
        ok, msg = save_device_attributes(device_id, form_model, payload)
        if not ok:
            flash(msg or "保存失败", "error")
        else:
            flash("保存成功", "success")
            return redirect(url_for("devices_bp.list_page"))

    return render_template("device_attrs_form.html", device=device, attrs=form_model)


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
