from flask import Blueprint, render_template, request, redirect, url_for, flash
from services.attribute_service import (
    ALLOWED_SCOPES, ALLOWED_DTYPES,
    list_attributes_paged, get_attribute, create_attribute, update_attribute, delete_attribute
)

# 创建一个名为 "attributes" 的蓝图，URL前缀为 "/attributes"
bp_attrs = Blueprint("attributes", __name__, url_prefix="/attributes")

# 属性列表页面路由
@bp_attrs.route("/", methods=["GET"])
def list_page():
    q = (request.args.get("q") or "").strip()  # 获取查询字符串并去除前后空格
    scope = request.args.get("scope") or ""  # 获取作用域
    dtype = request.args.get("data_type") or ""  # 获取数据类型
    page = request.args.get("page", default=1, type=int)  # 获取当前页数，默认为1
    page_size = request.args.get("page_size", default=10, type=int)  # 获取每页显示的数量，默认为10

    data, total = list_attributes_paged(q=q, scope=scope, data_type=dtype, page=page, page_size=page_size)  # 获取分页属性数据
    return render_template(
        "attributes_list.html",
        rows=data, total=total, page=page, page_size=page_size,  # 传递属性数据和分页信息
        q=q, scope=scope, data_type=dtype,  # 传递查询参数
        scopes=[""] + ALLOWED_SCOPES, dtypes=[""] + ALLOWED_DTYPES  # 传递作用域和数据类型选项
    )

# 新建属性页面路由
@bp_attrs.route("/new", methods=["GET", "POST"])
def create_page():
    if request.method == "POST":  # 如果是POST请求（表单提交）
        form = _parse_form(request)  # 解析表单数据
        try:
            create_attribute(**form)  # 创建新属性
            flash("已新增属性", "ok")  # 显示成功消息
            return redirect(url_for("attributes.list_page"))  # 重定向到属性列表页面
        except Exception as e:
            flash(f"保存失败：{e}", "err")  # 显示失败消息
    return render_template(
        "attributes_form.html",
        item=None, scopes=ALLOWED_SCOPES, dtypes=ALLOWED_DTYPES  # 传递作用域和数据类型选项
    )

# 编辑属性页面路由
@bp_attrs.route("/<int:attr_id>/edit", methods=["GET", "POST"])
def edit_page(attr_id):
    item = get_attribute(attr_id)  # 根据属性ID获取属性信息
    if not item:
        flash("属性不存在", "err")  # 如果属性不存在，显示错误消息
        return redirect(url_for("attributes.list_page"))  # 重定向到属性列表页面

    if request.method == "POST":  # 如果是POST请求（表单提交）
        form = _parse_form(request)  # 解析表单数据
        try:
            update_attribute(attr_id, **form)  # 更新属性信息
            flash("已保存修改", "ok")  # 显示成功消息
            return redirect(url_for("attributes.list_page"))  # 重定向到属性列表页面
        except Exception as e:
            flash(f"保存失败：{e}", "err")  # 显示失败消息

    return render_template(
        "attributes_form.html",
        item=item, scopes=ALLOWED_SCOPES, dtypes=ALLOWED_DTYPES  # 传递属性信息、作用域和数据类型选项
    )

# 删除属性路由
@bp_attrs.route("/<int:attr_id>/delete", methods=["POST"])
def remove(attr_id):
    try:
        delete_attribute(attr_id)  # 删除指定属性
        flash("已删除属性", "ok")  # 显示成功消息
    except Exception as e:
        flash(f"删除失败：{e}", "err")  # 显示失败消息
    return redirect(url_for("attributes.list_page"))  # 重定向到属性列表页面

# 解析表单数据的辅助函数
def _parse_form(req):
    code = (req.form.get("code") or "").strip()  # 获取属性代码并去除前后空格
    name = (req.form.get("name") or "").strip()  # 获取属性名称并去除前后空格
    scope = req.form.get("scope")  # 获取属性作用域
    data_type = req.form.get("data_type")  # 获取属性数据类型
    unit = (req.form.get("unit") or None)  # 获取属性单位
    allow_multi = int(req.form.get("allow_multi") or 0)  # 获取是否允许多值
    min_value = req.form.get("min_value")  # 获取属性最小值
    max_value = req.form.get("max_value")  # 获取属性最大值
    description = (req.form.get("description") or None)  # 获取属性描述

    # 处理最小值和最大值为空的情况，并转换为浮点数
    min_value = None if (min_value is None or str(min_value).strip() == "") else float(min_value)
    max_value = None if (max_value is None or str(max_value).strip() == "") else float(max_value)

    # 如果数据类型不是枚举类型，则不允许多值
    if data_type != "enum":
        allow_multi = 0

    # 验证必填字段
    if not code or not name:
        raise ValueError("code 与 name 必填")
    if scope not in ALLOWED_SCOPES:
        raise ValueError("scope 非法")
    if data_type not in ALLOWED_DTYPES:
        raise ValueError("data_type 非法")

    # 返回解析后的表单数据字典
    return dict(code=code, name=name, scope=scope, data_type=data_type,
                unit=unit, min_value=min_value, max_value=max_value,
                allow_multi=allow_multi, description=description)
