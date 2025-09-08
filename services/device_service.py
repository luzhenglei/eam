# services/device_service.py
import re
from db import get_conn
from typing import Dict, List
from services.option_service import list_options

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

def update_device_basic(device_id: int, name: str, model_code: str, new_template_id: int = None):
    """
    修改设备基本信息；
    - 仅改名称/型号：更新 device 表即可；
    - 若切换模板：需要清理设备/端口的属性值与端口实例，再按新模板重建端口实例。
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 查询当前模板
            cur.execute("SELECT template_id FROM device WHERE id=%s", (device_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("设备不存在")
            old_template_id = row["template_id"]

            # 先更新名称/型号
            cur.execute("UPDATE device SET name=%s, model_code=%s WHERE id=%s",
                        (name, model_code, device_id))

            # 若不换模板，结束
            if new_template_id is None or int(new_template_id) == int(old_template_id):
                return True

            # 切换模板：清理旧数据
            # 1) 清设备属性值
            cur.execute("DELETE FROM device_attr_value WHERE device_id=%s", (device_id,))
            # 2) 找出端口
            cur.execute("SELECT id FROM port WHERE device_id=%s", (device_id,))
            port_rows = cur.fetchall()
            port_ids = [r["id"] for r in port_rows] if port_rows else []
            if port_ids:
                # 2.1) 清端口属性值
                cur.execute(
                    "DELETE FROM port_attr_value WHERE port_id IN (%s)" % (
                        ",".join(["%s"] * len(port_ids))
                    ),
                    tuple(port_ids)
                )
                # 2.2) 删端口
                cur.execute(
                    "DELETE FROM port WHERE id IN (%s)" % (
                        ",".join(["%s"] * len(port_ids))
                    ),
                    tuple(port_ids)
                )

            # 3) 更新设备模板
            cur.execute("UPDATE device SET template_id=%s WHERE id=%s",
                        (new_template_id, device_id))

        # 4) 按新模板生成端口实例
        _ensure_ports_for_device(new_template_id, device_id)

    return True


def delete_device(device_id: int):
    """
    删除设备（含：端口属性值 -> 端口 -> 设备属性值 -> 设备）
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 找端口
            cur.execute("SELECT id FROM port WHERE device_id=%s", (device_id,))
            port_rows = cur.fetchall()
            port_ids = [r["id"] for r in port_rows] if port_rows else []

            if port_ids:
                # 先删端口属性值
                cur.execute(
                    "DELETE FROM port_attr_value WHERE port_id IN (%s)" % (
                        ",".join(["%s"] * len(port_ids))
                    ),
                    tuple(port_ids)
                )
                # 再删端口
                cur.execute(
                    "DELETE FROM port WHERE id IN (%s)" % (
                        ",".join(["%s"] * len(port_ids))
                    ),
                    tuple(port_ids)
                )

            # 删设备属性值
            cur.execute("DELETE FROM device_attr_value WHERE device_id=%s", (device_id,))

            # 删设备
            cur.execute("DELETE FROM device WHERE id=%s", (device_id,))

    return True


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
    把 device.category / device.category2 ... 统一归到 base 'device.category'
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
        # 容错
        if item.get("is_required") is None:
            item["is_required"] = 0
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
            "port": {
                "id": pid,
                "name": p.get("name") or f"Port-{pid}",
                "parent_port_id": p.get("parent_port_id"),
                "max_links": p.get("max_links") or 1,
            },
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
                    cur.execute(
                        f"""
                        SELECT id, {col} AS name, parent_port_id, max_links
                        FROM port
                        WHERE device_id=%s
                        ORDER BY COALESCE(parent_port_id, id), (parent_port_id IS NULL) DESC, id
                        """,
                        (device_id,),
                    )
                    rows = cur.fetchall()
                    # 若该列存在但值全是 NULL/空，也继续使用（前端会显示空字符串）
                    return rows
                except Exception:
                    # 列不存在时这里会报错，忽略并尝试下一列
                    conn.rollback()
                    continue
            # 兜底：只取 id，自造一个 name
            cur.execute(
                """
                SELECT id, parent_port_id, max_links
                FROM port
                WHERE device_id=%s
                ORDER BY COALESCE(parent_port_id, id), (parent_port_id IS NULL) DESC, id
                """,
                (device_id,),
            )
            rows = cur.fetchall()
            for r in rows:
                r["name"] = f"Port-{r['id']}"
            return rows


def create_child_port(device_id: int, parent_port_id: int, name: str) -> int:
    """在设备下为某端口新增子端口（仅允许一层）。"""
    if not name or not str(name).strip():
        raise ValueError("name required")
    name = str(name).strip()
    with get_conn() as conn, conn.cursor() as cur:
        # 检查父端口合法性
        cur.execute(
            "SELECT id, port_type_id, parent_port_id FROM port WHERE id=%s AND device_id=%s",
            (parent_port_id, device_id),
        )
        parent = cur.fetchone()
        if not parent:
            raise ValueError("parent port not found")
        if parent.get("parent_port_id"):
            raise ValueError("only one level of nesting allowed")

        # 名称冲突检查（同设备内）
        cur.execute(
            "SELECT 1 FROM port WHERE device_id=%s AND name=%s",
            (device_id, name),
        )
        if cur.fetchone():
            raise ValueError("port name already exists")

        # 插入子端口
        cur.execute(
            "INSERT INTO port (device_id, name, parent_port_id, port_type_id) VALUES (%s,%s,%s,%s)",
            (device_id, name, parent_port_id, parent.get("port_type_id")),
        )
        new_id = cur.lastrowid

        # 继承属性
        cur.execute(
            "SELECT attribute_id, option_id, value_text FROM port_attr_value WHERE port_id=%s",
            (parent_port_id,),
        )
        rows = cur.fetchall() or []
        for r in rows:
            cur.execute(
                "INSERT INTO port_attr_value (port_id, attribute_id, option_id, value_text) VALUES (%s,%s,%s,%s)",
                (new_id, r["attribute_id"], r["option_id"], r["value_text"]),
            )

        conn.commit()
        return new_id


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

                # 更新端口最大连接数
                raw_ml = payload.get(f"port_{port_id}_max_links")
                try:
                    ml = int(raw_ml)
                    if ml < 1:
                        ml = 1
                except Exception:
                    ml = 1
                cur.execute("UPDATE port SET max_links=%s WHERE id=%s", (ml, port_id))

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
    将设备端口与模板端口规则进行“增量同步”：
      - 模板无规则：不生成
      - 对每条规则：
          * 标签不空：目标是 标签+1..标签+qty
            - 统计设备中已有 ^标签(\d+)$ 的最大序号与数量，若不足则从 (max+1) 开始补齐到 qty
          * 标签为空：目标是 纯数字 1..qty（所有空标签规则共用一条递增序列）
            - 统计设备中已有 ^(\d+)$ 的最大序号与数量，按总量缺多少补多少（共享序列）
      - 不删除、不改名，只有“缺口补齐”
    """
    with get_conn() as conn, conn.cursor() as cur:
        # 取模板规则
        cur.execute("""
            SELECT id, code, qty, port_type_id, max_links
            FROM port_template
            WHERE template_id=%s
            ORDER BY sort_order, id
        """, (template_id,))
        rules = cur.fetchall() or []
        if not rules:
            return  # 无规则不生成

        # 读取设备现有端口（名字集合 + 为前缀/纯数字准备的统计）
        cur.execute("SELECT name FROM port WHERE device_id=%s", (device_id,))
        existing = [ (r["name"] or "").strip() for r in (cur.fetchall() or []) ]
        used_names = set(existing)

        # 预统计：各前缀当前最大序号、纯数字当前最大序号，以及当前计数
        prefix_max = {}   # 非空标签：code -> max_num
        prefix_count = {} # 非空标签：code -> count( ^code\d+$ )
        numeric_max = 0   # 空标签共享：max of ^\d+$
        numeric_count = 0 # 空标签共享：count of ^\d+$

        # 扫一遍现有名字，统计
        num_pat = re.compile(r'^(\d+)$')
        def make_pat(code): return re.compile(rf'^{re.escape(code)}(\d+)$')

        # 为减少正则编译，多收集规则里的非空 code
        nonempty_codes = [ (r.get("code") or "").strip() for r in rules if (r.get("code") or "").strip() ]
        nonempty_codes = list(dict.fromkeys(nonempty_codes))  # 去重保持顺序
        code_pats = { c: make_pat(c) for c in nonempty_codes }

        for nm in existing:
            m_num = num_pat.match(nm)
            if m_num:
                n = int(m_num.group(1))
                numeric_max = max(numeric_max, n)
                numeric_count += 1
                continue
            # 尝试匹配各前缀
            for code, pat in code_pats.items():
                m = pat.match(nm)
                if m:
                    n = int(m.group(1))
                    prefix_max[code] = max(prefix_max.get(code, 0), n)
                    prefix_count[code] = prefix_count.get(code, 0) + 1
                    break

        # 先处理“非空标签”的规则：各自补齐到 qty
        for r in rules:
            code = (r.get("code") or "").strip()
            qty  = int(r.get("qty") or 1)
            if qty < 1:
                continue
            if code:
                have = prefix_count.get(code, 0)
                need = max(0, qty - have)
                if need == 0:
                    continue
                start = prefix_max.get(code, 0) + 1
                ptype = r.get("port_type_id")
                ml = r.get("max_links") or 1
                # 补 need 个
                for i in range(need):
                    nm = f"{code}{start+i}"
                    # 极端保障：同事务内避重
                    while nm in used_names:
                        start += 1
                        nm = f"{code}{start+i}"
                    # 插入端口时：
                    if ptype:
                        cur.execute(
                        "INSERT INTO port (device_id, name, port_type_id, port_template_id, max_links) VALUES (%s, %s, %s, %s, %s)",
                        (device_id, nm, ptype, r["id"], ml)
                        )
                    else:
                        cur.execute(
                        "INSERT INTO port (device_id, name, port_template_id, max_links) VALUES (%s, %s, %s, %s)",
                        (device_id, nm, r["id"], ml)
                        )

                    used_names.add(nm)
                # 更新统计缓存
                prefix_count[code] = have + need
                prefix_max[code] = start + need - 1

        # 再处理“空标签”的规则：共享纯数字序列，按总量补齐到 sum(qty)
        total_empty_qty = sum(int(r.get("qty") or 1) for r in rules if not (r.get("code") or "").strip())
        need_empty = max(0, total_empty_qty - numeric_count)
        if need_empty > 0:
            start = numeric_max + 1
            for i in range(need_empty):
                nm = f"{start+i}"
                while nm in used_names:
                    start += 1
                    nm = f"{start+i}"
                cur.execute(
                    "INSERT INTO port (device_id, name, max_links) VALUES (%s, %s, %s)",
                    (device_id, nm, 1)
                )
                used_names.add(nm)

        conn.commit()
          
# === port_template CRUD ===

def list_port_templates(template_id: int):
    with get_conn() as conn, conn.cursor() as cur:
        # 带出 port_type_id 与类型名，方便 UI 展示
        cur.execute("""
            SELECT pt.id, pt.template_id, pt.code, pt.name,
                   pt.port_type_id,
                   t.name AS port_type_name,
                   pt.qty, pt.naming_rule, pt.sort_order, pt.max_links
            FROM port_template pt
            LEFT JOIN port_type t ON t.id = pt.port_type_id
            WHERE pt.template_id=%s
            ORDER BY pt.sort_order, pt.id
        """, (template_id,))
        return cur.fetchall()

def create_port_template(template_id: int, code: str, name: str,
                         qty: int = 1, naming_rule: str = None,
                         sort_order: int = 0, port_type_id: int = None,
                         max_links: int = 1):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO port_template (template_id, code, name, port_type_id, qty, naming_rule, sort_order, max_links)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (template_id, code, name, port_type_id, qty, naming_rule, sort_order, max_links))
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



def list_device_ports_with_type(device_id: int):
    """
    返回设备端口（含端口类型），用于分组展示。
    [{'id':1,'name':'PW1','port_type_id':3,'port_type_name':'电源端口'}, ...]
    """
    sql = """
    SELECT p.id, p.name,
           pt.id  AS port_type_id,
           pt.name AS port_type_name
    FROM port p
    LEFT JOIN port_type pt ON pt.id = p.port_type_id
    WHERE p.device_id=%s
    ORDER BY p.id
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (device_id,))
        return cur.fetchall()

def _option_name_map(attribute_id: int) -> Dict[int, str]:
    """把某个属性的所有选项做成 {id: name} 的字典，便于展示。"""
    mapping = {}
    for op in list_options(attribute_id):
        mapping[op["id"]] = op["name"]
    return mapping



def _option_name_map(attribute_id: int) -> Dict[int, str]:
    """把某个属性的所有选项做成 {id: name} 的字典，便于展示。"""
    mapping = {}
    for op in list_options(attribute_id):
        mapping[op["id"]] = op["name"]
    return mapping

def get_device_preview_data(device_id: int):
    """
    预览页数据（树状）：
    - 顶部：设备ID/名称/类型/模板
    - 设备属性：
        * 非枚举：name -> value
        * 级联枚举：name -> 路径（用 › 连接）
    - 端口（按模板端口规则三层展示）：
        * 第一层：端口类型（port_type.name，空则“未分类”）
        * 第二层：属性（port_template.name）
        * 第三层：按规则生成的“标签+序号”（code1..codeN）
          —— 这里按规则预览，不依赖已生成实例
    """
    device = get_device(device_id)
    if not device:
        raise ValueError("设备不存在")
    template_id = device.get("template_id")
    if not template_id:
        raise ValueError("该设备未绑定模板")

    # 复用已有模型拿到属性与当前值
    model = get_template_attrs_for_form(template_id, device_id)

    # ===== 设备属性：只用“名称”做键 =====
    flat_items = []
    for a in model.get("flat_attrs", []):
        name = a["name"]  # 只展示属性名称
        dt = a["data_type"]
        cur = a.get("current", {}) or {}
        if dt == "enum":
            ids = cur.get("enum_option_ids") or []
            if ids:
                names = [ _option_name_map(a["attribute_id"]).get(i, str(i)) for i in ids ]
                value = "，".join([n for n in names if n])
            else:
                value = ""
        else:
            value = cur.get("value_text") or ""
        flat_items.append({"name": name, "value": value})

    # --- 设备属性：级联，优先显示文本 ---
    cascaded_items = []
    casc_groups = model.get("cascaded_groups", []) or []
    if casc_groups:
        # 取这些属性的“显示名称”
        aid_list = [ (g.get("tree_attr_id") or g.get("attribute_id")) for g in casc_groups if (g.get("tree_attr_id") or g.get("attribute_id")) ]
        aid_to_name = {}
        if aid_list:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT id, name FROM attribute_def WHERE id IN (%s)" % ",".join(["%s"]*len(aid_list)),
                    tuple(aid_list)
                )
                for r in cur.fetchall() or []:
                    aid_to_name[r["id"]] = r["name"]

        for g in casc_groups:
            aid = g.get("tree_attr_id") or g.get("attribute_id")
            if not aid:
                continue
            attr_display_name = aid_to_name.get(aid, "")  # 只用“属性名称”
            chain_ids = g.get("selected_chain") or []
            texts = g.get("texts") or {}
            level_texts = (texts.get("levels") or [])
            root_text = (texts.get("root") or "").strip()

            # 优先用文本：root + levels
            text_path = [t.strip() for t in ([root_text] + level_texts) if t and t.strip()]
            if text_path:
                path = " › ".join(text_path)
            else:
                # 文本为空才退回到选项名
                name_map = _option_name_map(aid)
                path = " › ".join([name_map.get(oid, str(oid)) for oid in chain_ids if oid])

            cascaded_items.append({"name": attr_display_name, "path": path})


    # ===== 端口：按“模板规则”三层（端口类型 -> 属性 -> 标签+序号） =====
    rules = list_port_templates(template_id)  # 需返回 id, code, name(属性), qty, port_type_name
    ports_tree = {}  # {ptype: { attr_name: [ 'PW1','PW2',... ] } }
    for r in rules:
        ptype = r.get("port_type_name") or "未分类"
        attr  = r.get("name") or "（未命名属性）"   # 模板规则中的“属性”字段
        code  = (r.get("code") or "").strip()
        qty   = int(r.get("qty") or 1)
        if qty < 1:  # 容错
            continue
        # 第三层：生成 代码+序号，单个也从 1 开始
        if code:
            names = [ f"{code}{i}" for i in range(1, qty+1) ]
        else:
            # 若标签允许为空，按纯数字预览
            names = [ f"{i}" for i in range(1, qty+1) ]
        ports_tree.setdefault(ptype, {}).setdefault(attr, []).extend(names)

    return {
        "device": {
            "project_id": device.get("project_id"),
            "id": device["id"],
            "name": device["name"],
            "device_type": device.get("device_type") or "",
            "template_name": device.get("template_name") or "",
        },
        "device_attrs": {"flat": flat_items, "cascaded": cascaded_items},
        "ports_tree": ports_tree
    }
    
    
def list_devices_by_project(project_id: int):
    sql = """
    SELECT d.id, d.name, d.model_code, d.template_id,
           dt.name AS template_name, dt.device_type
    FROM device d
    LEFT JOIN device_template dt ON dt.id = d.template_id
    WHERE d.project_id=%s
    ORDER BY d.id DESC
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (project_id,))
        return cur.fetchall()

def create_device_in_project(project_id: int, template_id: int, name: str, model_code: str):
    sql = "INSERT INTO device (project_id, template_id, name, model_code) VALUES (%s, %s, %s, %s)"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (project_id, template_id, name, model_code))
        return cur.lastrowid


def search_devices_in_project(project_id: int, keyword: str):
    kw = f"%{(keyword or '').strip()}%"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT id, name, model_code
          FROM device
          WHERE project_id=%s
            AND (name LIKE %s OR model_code LIKE %s)
          ORDER BY id DESC
        """, (project_id, kw, kw))
        return cur.fetchall()

