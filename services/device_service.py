# services/device_service.py
import re
from db import get_conn
from services.option_service import list_options  # 用于枚举属性构建 options 列表

# -------- 设备基础 --------

def list_devices():
    sql = """
    SELECT d.id, d.name, d.template_id, d.model_code,
           dt.name AS template_name, dt.device_type
    FROM device d
    LEFT JOIN device_template dt ON dt.id = d.template_id
    ORDER BY d.id DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

def get_device(device_id: int):
    sql = """
    SELECT d.*, dt.name AS template_name, dt.device_type
    FROM device d
    LEFT JOIN device_template dt ON dt.id = d.template_id
    WHERE d.id=%s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (device_id,))
            return cur.fetchone()

def create_device_basic(template_id: int, name: str, model_code: str):
    sql = "INSERT INTO device (template_id, name, model_code) VALUES (%s, %s, %s)"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (template_id, name, model_code))
            return cur.lastrowid

# -------- 表单模型构建（设备级） --------

def _list_template_device_attrs(template_id: int):
    """
    拉取模板下 scope=device 的属性；模板包含的优先显示，其余为可选。
    """
    sql = """
    SELECT ad.id AS attribute_id, ad.code, ad.name, ad.data_type, ad.allow_multi,
           ta.is_required
    FROM attribute_def ad
    LEFT JOIN template_attribute ta
           ON ta.attribute_id = ad.id AND ta.template_id = %s
    WHERE ad.scope='device'
    ORDER BY (ta.is_required IS NOT NULL) DESC, ad.id DESC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (template_id,))
            return cur.fetchall()

# services/device_service.py

def _get_current_values(device_id: int):
    """
    读取 device_attr_value，返回：
    {
      attribute_id: {
        "enum_option_ids": [..],   # 所有 option_id（按写入顺序）
        "value_text": "..." or "", # 供非枚举/单值回显（取最后一个非空文本）
        "texts": ["..",".."]       # （可选）保留所有文本，给需要时用
      },
      ...
    }
    兼容：同一 attribute_id 可能有多行（单属性树的每级一行；或多选枚举）。
    """
    res = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT attribute_id, option_id, value_text
                FROM device_attr_value
                WHERE device_id=%s
                ORDER BY id
            """, (device_id,))
            rows = cur.fetchall()

    for row in rows:
        aid = row["attribute_id"]
        d = res.setdefault(aid, {"enum_option_ids": [], "value_text": "", "texts": []})
        # 枚举/级联：收集 option_id
        if row["option_id"] is not None:
            d["enum_option_ids"].append(row["option_id"])
        # 文本：保留到 texts，最后一个非空作为 value_text 回显
        if row["value_text"] is not None and str(row["value_text"]).strip() != "":
            d["texts"].append(row["value_text"])

    # 归一化 value_text（给非枚举/单值控件使用）
    for aid, d in res.items():
        d["value_text"] = d["texts"][-1] if d["texts"] else ""

    return res

def _base_code(code: str):
    """
    把 device.category / device.category1 / device.category2 ... 统一归到 base 'device.category'
    规则：去掉末尾的连续数字。
    """
    if code is None:
        return None
    return re.sub(r"\d+$", "", code)

def _group_cascaded(attrs: list):
    """
    根据 code 前缀把多属性级联分组：
    { base_code: [ (level, item_dict), ... ] }
    level 定义：根属性 level=0；其后缀数字就是 level（如 category1 -> level=1）
    """
    groups = {}
    for it in attrs:
        code = it["code"] or ""
        base = _base_code(code)
        # 计算 level：去掉 base 剩下的是数字；根属性 level=0
        suffix = code[len(base):]
        try:
            level = int(suffix) if suffix else 0
        except:
            level = 0
        groups.setdefault(base, []).append((level, it))
    # 每组按 level 从小到大排序
    for k in groups:
        groups[k].sort(key=lambda x: x[0])
    return groups

def get_template_attrs_for_form(template_id: int, device_id: int):
    """
    返回用于设备属性编辑页面的数据：
    {
      "flat_attrs": [...],
      "cascaded_groups": [
        {
          "base": "device.category",
          "tree_attr_id": <int>,
          "selected_chain": [oid1, oid2, ...],           # 已选链
          "texts": { "root": "<根文本>", "levels": ["..","..", ...] }  # 各级文本，顺序与 selected_chain 对齐
        },
        ...
      ]
    }
    """
    attrs = _list_template_device_attrs(template_id)
    current = _get_current_values(device_id)

    # 为普通属性准备 options/current
    for item in attrs:
        if item["data_type"] == "enum":
            opts = [o for o in list_options(item["attribute_id"]) if (o.get("code") or "") != "__root__"]
            item["options"] = [{"id": o["id"], "name": o["name"], "parent_id": o["parent_id"]} for o in opts]
        else:
            item["options"] = []
        item["current"] = current.get(item["attribute_id"], {"enum_option_ids": [], "value_text": ""})
        if item["is_required"] is None:
            item["is_required"] = 0

    flat_attrs, cascaded_groups = [], []

    for a in attrs:
        # 单属性树：以 code == 'device.category' 为例（你可按需扩展规则）
        if a["data_type"] == "enum" and a["code"] == "device.category":
            aid = a["attribute_id"]

            # 读取该属性的所有记录（用于构造链与文本）
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT option_id, value_text
                        FROM device_attr_value
                        WHERE device_id=%s AND attribute_id=%s
                        ORDER BY id
                    """, (device_id, aid))
                    rows = cur.fetchall()

            # 构造链（所有非空 option_id，按写入顺序）
            chain_ids = [r["option_id"] for r in rows if r["option_id"] is not None]

            # 根文本 = option_id 为 NULL 的记录里“最后一个非空文本”
            root_text = ""
            for r in rows:
                if r["option_id"] is None and r["value_text"] is not None and str(r["value_text"]).strip() != "":
                    root_text = r["value_text"]

            # 为每个 option_id 找到“最后一个非空文本”（按链顺序对齐）
            last_text_by_oid = {}
            for r in rows:
                oid = r["option_id"]
                if oid is None:
                    continue
                vt = r["value_text"]
                if vt is not None and str(vt).strip() != "":
                    last_text_by_oid[oid] = vt
            level_texts = [last_text_by_oid.get(oid, "") for oid in chain_ids]

            cascaded_groups.append({
                "base": a["code"],
                "tree_attr_id": aid,
                "selected_chain": chain_ids,
                "texts": {"root": root_text, "levels": level_texts},
            })
        else:
            flat_attrs.append(a)

    return {"flat_attrs": flat_attrs, "cascaded_groups": cascaded_groups}

# -------- 保存 --------

def save_device_attributes(device_id: int, form_model: dict, payload: dict):
    """
    兼容两类表单：
    1) 普通属性（flat_attrs）
       - 枚举单选：  <select name="attr_{aid}">value=option_id</select>
       - 枚举多选：  <select name="attr_{aid}" multiple>...</select>  （payload.getlist 已在上层 to_dict 前消失，这里兼容 list/str）
       - 非枚举：    <input  name="attr_{aid}" type="text">
    2) 单属性 + 选项树级联（cascaded_groups）
       - group_{base}_chain: "oid0,oid1,..."
       - attr_{attrId}_text_root: 根文本（可空、始终可见）
       - attr_{attrId}_text_{i}:  第 i 层文本（选了非空才出现 → 必填）
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # -------- 普通属性（flat_attrs）--------
            for item in form_model.get("flat_attrs", []):
                aid = item["attribute_id"]
                dtype = item["data_type"]
                allow_multi = bool(item["allow_multi"])

                # 统一先删旧
                cur.execute(
                    "DELETE FROM device_attr_value WHERE device_id=%s AND attribute_id=%s",
                    (device_id, aid)
                )

                if dtype == "enum":
                    # 兼容单选/多选：payload["attr_{aid}"] 可能是 str 或 list，也可能不存在
                    raw = payload.get(f"attr_{aid}")
                    ids = []
                    if isinstance(raw, list):
                        ids = [int(x) for x in raw if str(x).strip().isdigit()]
                    elif isinstance(raw, str) and raw.strip():
                        ids = [int(raw)]
                    # 去重
                    ids = list(dict.fromkeys(ids))
                    if not ids:
                        continue
                    if not allow_multi and len(ids) > 1:
                        ids = ids[:1]
                    for oid in ids:
                        cur.execute(
                            "INSERT INTO device_attr_value (device_id, attribute_id, option_id, value_text) "
                            "VALUES (%s,%s,%s,NULL)",
                            (device_id, aid, oid)
                        )
                else:
                    v = (payload.get(f"attr_{aid}") or "").strip()
                    if v != "":
                        cur.execute(
                            "INSERT INTO device_attr_value (device_id, attribute_id, option_id, value_text) "
                            "VALUES (%s,%s,NULL,%s)",
                            (device_id, aid, v)
                        )

            # -------- 单属性 + 树（cascaded_groups）--------
            for g in form_model.get("cascaded_groups", []):
                base = g.get("base")
                attr_id = g.get("tree_attr_id") or g.get("attribute_id")  # 兼容字段名
                if not base or not attr_id:
                    continue

                # 解析链
                chain_key = f"group_{base}_chain"
                chain_raw = payload.get(chain_key, "") or ""
                chain_ids = [int(x) for x in chain_raw.split(",") if str(x).strip().isdigit()]

                # 根文本
                root_text_key = f"attr_{attr_id}_text_root"
                root_text = (payload.get(root_text_key) or "").strip()

                # 清空该属性所有旧值
                cur.execute(
                    "DELETE FROM device_attr_value WHERE device_id=%s AND attribute_id=%s",
                    (device_id, attr_id)
                )

                # 写根文本（可空）
                if root_text != "":
                    cur.execute(
                        "INSERT INTO device_attr_value (device_id, attribute_id, option_id, value_text) "
                        "VALUES (%s,%s,NULL,%s)",
                        (device_id, attr_id, root_text)
                    )

                # 逐层写入（选了非空项则该层文本必填）
                for i, oid in enumerate(chain_ids):
                    txt_key = f"attr_{attr_id}_text_{i}"
                    txt_val = (payload.get(txt_key) or "").strip()
                    if txt_val == "":
                        return False, f"第 {i+1} 级已选择项需填写取值"
                    cur.execute(
                        "INSERT INTO device_attr_value (device_id, attribute_id, option_id, value_text) "
                        "VALUES (%s,%s,%s,%s)",
                        (device_id, attr_id, oid, txt_val)
                    )

    return True, ""
