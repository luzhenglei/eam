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
    返回页面模型：
    {
      "flat_attrs": [...],                 # 设备(scope='device') 普通属性
      "cascaded_groups": [...],            # 设备 单属性树级联（凡是“枚举且存在层级”的属性）
      "ports": [                           # 端口侧（每个端口一组）
        {
          "port": {"id":..., "name":...},
          "flat_attrs": [...],             # 端口普通属性
          "cascaded_groups": [             # 端口 单属性树级联
            {
              "base": "<attr_code>",
              "tree_attr_id": <attribute_id>,
              "selected_chain": [optionId1, optionId2, ...],
              "texts": {
                "root": "<根文本>",
                "levels": ["第1级文本","第2级文本", ...]   # 与 selected_chain 顺序一致
              }
            },
            ...
          ]
        },
        ...
      ]
    }
    """
    # ===== 设备侧：属性定义 + 当前值 =====
    attrs = _list_template_device_attrs(template_id)
    current = _get_current_values(device_id)

    # 为设备属性补充 options 与 current
    for item in attrs:
        if item["data_type"] == "enum":
            opts = [o for o in list_options(item["attribute_id"]) if (o.get("code") or "") != "__root__"]
            item["options"] = [{"id": o["id"], "name": o["name"], "parent_id": o["parent_id"]} for o in opts]
        else:
            item["options"] = []
        item["current"] = current.get(item["attribute_id"], {"enum_option_ids": [], "value_text": ""})
        # 容错：某些库 is_required 可能为 NULL
        if item.get("is_required") is None:
            item["is_required"] = 0
        # 容错：allow_multi 缺失时按 0 处理
        if item.get("allow_multi") is None:
            item["allow_multi"] = 0

    # 设备侧分流：凡“枚举且存在父子层级”的属性 → 单属性树；其余进普通表格
    flat_attrs, cascaded_groups = [], []
    for a in attrs:
        if a["data_type"] == "enum" and _has_option_hierarchy(a["attribute_id"]):
            aid = a["attribute_id"]
            # 读取该属性在该设备下的所有记录，以构造链与回显文本
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT option_id, value_text
                    FROM device_attr_value
                    WHERE device_id=%s AND attribute_id=%s
                    ORDER BY id
                """, (device_id, aid))
                rows = cur.fetchall()

            # selected_chain：所有非空 option_id，保持插入顺序
            chain_ids = [r["option_id"] for r in rows if r["option_id"] is not None]

            # 根文本：option_id 为 NULL 的记录里最后一个非空文本
            root_text = ""
            for r in rows:
                if r["option_id"] is None and r["value_text"] and str(r["value_text"]).strip():
                    root_text = r["value_text"]

            # 每级文本：按链顺序提取“该 option_id 的最后一个非空文本”
            last_text_by_oid = {}
            for r in rows:
                if r["option_id"] and r["value_text"] and str(r["value_text"]).strip():
                    last_text_by_oid[r["option_id"]] = r["value_text"]
            level_texts = [last_text_by_oid.get(oid, "") for oid in chain_ids]

            cascaded_groups.append({
                "base": a["code"],                 # 使用属性自身 code 作为组名
                "tree_attr_id": aid,
                "selected_chain": chain_ids,
                "texts": {"root": root_text, "levels": level_texts},
            })
        else:
            flat_attrs.append(a)

    # ===== 端口侧：端口清单 + 端口属性定义 + 当前值 =====
    ports_model = []
    ports = _list_device_ports(device_id)                 # [{"id":..,"name":..}, ...]
    port_attrs_all = _list_template_port_attrs(template_id)  # 端口作用域的属性定义

    for p in ports:
        pid = p["id"]
        curvals = _get_current_port_values(pid)

        pa_flat, pa_cascade = [], []
        for a in port_attrs_all:
            a = dict(a)  # 拷贝一份，避免污染原对象

            # options + current
            if a["data_type"] == "enum":
                opts = [o for o in list_options(a["attribute_id"]) if (o.get("code") or "") != "__root__"]
                a["options"] = [{"id": o["id"], "name": o["name"], "parent_id": o["parent_id"]} for o in opts]
            else:
                a["options"] = []
            a["current"] = curvals.get(a["attribute_id"], {"enum_option_ids": [], "value_text": ""})
            if a.get("is_required") is None:
                a["is_required"] = 0
            if a.get("allow_multi") is None:
                a["allow_multi"] = 0

            # 端口侧分流：凡“枚举且存在父子层级”的属性 → 单属性树
            if a["data_type"] == "enum" and _has_option_hierarchy(a["attribute_id"]):
                aid = a["attribute_id"]
                with get_conn() as conn, conn.cursor() as cur:
                    cur.execute("""
                        SELECT option_id, value_text
                        FROM port_attr_value
                        WHERE port_id=%s AND attribute_id=%s
                        ORDER BY id
                    """, (pid, aid))
                    rows = cur.fetchall()

                chain_ids = [r["option_id"] for r in rows if r["option_id"] is not None]

                root_text = ""
                for r in rows:
                    if r["option_id"] is None and r["value_text"] and str(r["value_text"]).strip():
                        root_text = r["value_text"]

                last_text_by_oid = {}
                for r in rows:
                    if r["option_id"] and r["value_text"] and str(r["value_text"]).strip():
                        last_text_by_oid[r["option_id"]] = r["value_text"]
                level_texts = [last_text_by_oid.get(oid, "") for oid in chain_ids]

                pa_cascade.append({
                    "base": a["code"],
                    "tree_attr_id": aid,
                    "selected_chain": chain_ids,
                    "texts": {"root": root_text, "levels": level_texts},
                })
            else:
                pa_flat.append(a)

        ports_model.append({
            "port": {"id": pid, "name": p.get("name") or f"Port-{pid}"},
            "flat_attrs": pa_flat,
            "cascaded_groups": pa_cascade
        })

    # 汇总返回
    return {
        "flat_attrs": flat_attrs,
        "cascaded_groups": cascaded_groups,
        "ports": ports_model
    }


def _get_current_port_values(port_id: int):
    """读取 port_attr_value，结构与 _get_current_values 相同。"""
    res = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT attribute_id, option_id, value_text
            FROM port_attr_value
            WHERE port_id=%s
            ORDER BY id
        """, (port_id,))
        rows = cur.fetchall()
    for r in rows:
        aid = r["attribute_id"]
        d = res.setdefault(aid, {"enum_option_ids": [], "value_text": "", "texts": []})
        if r["option_id"] is not None:
            d["enum_option_ids"].append(r["option_id"])
        if r["value_text"] and str(r["value_text"]).strip():
            d["texts"].append(r["value_text"])
    for d in res.values():
        d["value_text"] = d["texts"][-1] if d["texts"] else ""
    return res

def _list_device_ports(device_id: int):
    """
    列出某设备下的端口实例。
    尝试 name/port_name/port_no/code/label 任一列作为显示名；都没有则用 'Port-{id}'。
    """
    candidates = ("name", "port_name", "port_no", "code", "label")
    with get_conn() as conn:
        with conn.cursor() as cur:
            for col in candidates:
                try:
                    # 注意：col 来自受控白名单，非用户输入
                    cur.execute(f"SELECT id, {col} AS name FROM port WHERE device_id=%s ORDER BY id", (device_id,))
                    rows = cur.fetchall()
                    # 若该列存在但值全是 NULL/空，也继续使用（前端会显示空字符串）
                    return rows
                except Exception:
                    # 列不存在时这里会报错，忽略并尝试下一列
                    conn.rollback()
                    continue
            # 兜底：只取 id，自造一个 name
            cur.execute("SELECT id FROM port WHERE device_id=%s ORDER BY id", (device_id,))
            rows = cur.fetchall()
            for r in rows:
                r["name"] = f"Port-{r['id']}"
            return rows


def _list_template_port_attrs(template_id: int):
    """
    列出模板绑定的端口作用域(scope='port')属性定义。
    只依赖通用字段：attribute_def(id, code, name, scope, data_type)
    template_attribute(template_id, attribute_id, is_required)
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    ad.id        AS attribute_id,
                    ad.code      AS code,
                    ad.name      AS name,
                    ad.data_type AS data_type,
                    0            AS allow_multi,        -- 你的表若没有 allow_multi，这里固定为 0
                    COALESCE(ta.is_required, 0) AS is_required
                FROM template_attribute ta
                JOIN attribute_def ad ON ad.id = ta.attribute_id
                WHERE ta.template_id = %s
                  AND ad.scope = 'port'
                ORDER BY ad.id
            """, (template_id,))
            return cur.fetchall()

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
                        # -------- 端口侧保存（与设备“单属性树/普通属性”同思路，但带 port_id 前缀） --------
            for p in form_model.get("ports", []):
                port_id = p["port"]["id"]

                # 普通端口属性
                for item in p.get("flat_attrs", []):
                    aid = item["attribute_id"]
                    dtype = item["data_type"]
                    allow_multi = bool(item["allow_multi"])

                    # 先删旧
                    cur.execute("DELETE FROM port_attr_value WHERE port_id=%s AND attribute_id=%s",
                                (port_id, aid))

                    if dtype == "enum":
                        raw = payload.get(f"port_{port_id}_attr_{aid}")
                        ids = []
                        if isinstance(raw, list):
                            ids = [int(x) for x in raw if str(x).strip().isdigit()]
                        elif isinstance(raw, str) and raw.strip():
                            ids = [int(raw)]
                        ids = list(dict.fromkeys(ids))
                        if not ids:
                            continue
                        if not allow_multi and len(ids) > 1:
                            ids = ids[:1]
                        for oid in ids:
                            cur.execute(
                                "INSERT INTO port_attr_value (port_id, attribute_id, option_id, value_text) "
                                "VALUES (%s,%s,%s,NULL)",
                                (port_id, aid, oid)
                            )
                    else:
                        v = (payload.get(f"port_{port_id}_attr_{aid}") or "").strip()
                        if v != "":
                            cur.execute(
                                "INSERT INTO port_attr_value (port_id, attribute_id, option_id, value_text) "
                                "VALUES (%s,%s,NULL,%s)",
                                (port_id, aid, v)
                            )

                # 端口侧单属性树（如 port.network）
                for g in p.get("cascaded_groups", []):
                    base = g.get("base")
                    attr_id = g.get("tree_attr_id") or g.get("attribute_id")
                    if not base or not attr_id:
                        continue

                    chain_key = f"port_{port_id}_group_{base}_chain"
                    chain_raw = payload.get(chain_key, "") or ""
                    chain_ids = [int(x) for x in chain_raw.split(",") if str(x).strip().isdigit()]

                    root_text_key = f"port_{port_id}_attr_{attr_id}_text_root"
                    root_text = (payload.get(root_text_key) or "").strip()

                    # 清空
                    cur.execute("DELETE FROM port_attr_value WHERE port_id=%s AND attribute_id=%s",
                                (port_id, attr_id))

                    if root_text != "":
                        cur.execute(
                            "INSERT INTO port_attr_value (port_id, attribute_id, option_id, value_text) "
                            "VALUES (%s,%s,NULL,%s)",
                            (port_id, attr_id, root_text)
                        )

                    for i, oid in enumerate(chain_ids):
                        txt_key = f"port_{port_id}_attr_{attr_id}_text_{i}"
                        txt_val = (payload.get(txt_key) or "").strip()
                        if txt_val == "":
                            return False, f"端口 {port_id}：级联第 {i+1} 级已选择项需填写取值"
                        cur.execute(
                            "INSERT INTO port_attr_value (port_id, attribute_id, option_id, value_text) "
                            "VALUES (%s,%s,%s,%s)",
                            (port_id, attr_id, oid, txt_val)
                        )


    return True, ""

def _ensure_ports_for_device(template_id: int, device_id: int):
    """
    若该设备还没有任何端口，则根据 port_template 为其生成端口实例。
    - 支持 qty=1 的单个端口
    - 支持 naming_rule 批量生成，如 'GE0/0/{i}'
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 已有端口？
            cur.execute("SELECT COUNT(*) AS c FROM port WHERE device_id=%s", (device_id,))
            exists = cur.fetchone()["c"]
            if exists and exists > 0:
                return  # 已存在则不重复生成

            # 取模板端口规则
            cur.execute("""
                SELECT id, code, name, qty, naming_rule
                FROM port_template
                WHERE template_id=%s
                ORDER BY sort_order, id
            """, (template_id,))
            rules = cur.fetchall()
            if not rules:
                return

            # 生成端口
            for r in rules:
                qty = int(r["qty"] or 1)
                rule = (r["naming_rule"] or "").strip()
                base_name = (r["name"] or "").strip()

                if qty <= 1 and base_name:
                    cur.execute("INSERT INTO port (device_id, name) VALUES (%s, %s)", (device_id, base_name))
                elif qty > 1:
                    for i in range(1, qty + 1):
                        if rule:
                            nm = rule.replace("{i}", str(i))
                        else:
                            nm = f"{base_name}{i}" if base_name else f"port{i}"
                        cur.execute("INSERT INTO port (device_id, name) VALUES (%s, %s)", (device_id, nm))
                else:
                    # 兜底
                    cur.execute("INSERT INTO port (device_id, name) VALUES (%s, %s)", (device_id, f"port-{r['id']}"))

            # 提交
            conn.commit()
            
# === port_template CRUD ===

def list_port_templates(template_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, template_id, code, name, qty, naming_rule, sort_order
            FROM port_template
            WHERE template_id=%s
            ORDER BY sort_order, id
        """, (template_id,))
        return cur.fetchall()

def create_port_template(template_id: int, code: str, name: str, qty: int = 1, naming_rule: str = None, sort_order: int = 0):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO port_template (template_id, code, name, qty, naming_rule, sort_order)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (template_id, code, name, qty, naming_rule, sort_order))
        return cur.lastrowid

def delete_port_template(pt_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM port_template WHERE id=%s", (pt_id,))
        return True


def _has_option_hierarchy(attribute_id: int) -> bool:
    """判断该枚举属性是否存在父子层级（有非空 parent_id 的选项）"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM attribute_option
            WHERE attribute_id=%s AND parent_id IS NOT NULL
            LIMIT 1
        """, (attribute_id,))
        return cur.fetchone() is not None

