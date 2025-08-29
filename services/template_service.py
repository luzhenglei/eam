from db import get_conn  # 引入数据库连接函数

# 列出所有设备模板
def list_templates():
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute("SELECT id, name, device_type, version, is_locked FROM device_template ORDER BY id DESC")  # 执行查询以获取所有设备模板，并按ID降序排列
            return cur.fetchall()  # 返回查询结果

# 列出指定模板的属性，根据提供的作用域过滤
def list_template_attributes(template_id, scope=None):
    sql = """SELECT ad.id AS attribute_id, ad.code, ad.name, ad.scope, ad.data_type, 
                 COALESCE(ta.is_required, NULL) AS is_required -- NULL 表示未挂到模板
             FROM attribute_def ad
             LEFT JOIN template_attribute ta
               ON ta.attribute_id = ad.id AND ta.template_id = %s"""  # 基础SQL查询语句，用于获取指定模板的属性，并检查属性是否为必填
    args = [template_id]  # 参数列表
    if scope in ("device", "port"):  # 如果提供了作用域且作用域在允许的范围内
        sql += " WHERE ad.scope=%s"  # 添加作用域过滤条件
        args.append(scope)  # 将作用域添加到参数列表
    sql += " ORDER BY ad.scope, ad.id DESC"  # 按作用域和ID降序排列结果
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute(sql, args)  # 执行查询
            return cur.fetchall()  # 返回查询结果

# 插入或更新指定模板的属性
def upsert_template_attributes(template_id, include_ids, required_ids, scope=None):
    """
    仅在【指定 scope】下更新绑定：
    - 删除：该模板且 scope=指定scope 的记录中，不在 include_ids 的全部删除
    - 插入/更新：include_ids 中的全部 upsert，is_required 取决于 required_ids
    """
    include_ids = set(int(x) for x in include_ids) if include_ids else set()
    required_ids = set(int(x) for x in required_ids) if required_ids else set()

    # 守护：scope 只接受 device/port；若传其它值则按 None 不加过滤（但此页面会传对）
    scope = scope if scope in ("device", "port") else None

    with get_conn() as conn:
        with conn.cursor() as cur:
            # —— 删除（仅限该 scope）——
            if scope:
                if include_ids:
                    in_clause = ",".join(["%s"] * len(include_ids))
                    # 只删除：属于该模板 & 属于该scope & 不在 include_ids 的
                    sql_del = f"""
                        DELETE ta FROM template_attribute ta
                        JOIN attribute_def ad ON ad.id = ta.attribute_id
                        WHERE ta.template_id = %s
                          AND ad.scope = %s
                          AND ta.attribute_id NOT IN ({in_clause})
                    """
                    cur.execute(sql_del, (template_id, scope, *include_ids))
                else:
                    # 本次一个都不包含 → 仅清空该 scope 下的绑定；不影响另一个 scope
                    sql_del = """
                        DELETE ta FROM template_attribute ta
                        JOIN attribute_def ad ON ad.id = ta.attribute_id
                        WHERE ta.template_id = %s
                          AND ad.scope = %s
                    """
                    cur.execute(sql_del, (template_id, scope))
            else:
                # 没有 scope（理论上不会走到），则保持旧行为（全量清空/删除）
                if include_ids:
                    in_clause = ",".join(["%s"] * len(include_ids))
                    cur.execute(f"DELETE FROM template_attribute WHERE template_id=%s AND attribute_id NOT IN ({in_clause})",
                                (template_id, *include_ids))
                else:
                    cur.execute("DELETE FROM template_attribute WHERE template_id=%s", (template_id,))

            # —— 插入/更新（upsert）——
            for aid in include_ids:
                is_req = 1 if aid in required_ids else 0
                cur.execute("""
                    INSERT INTO template_attribute (template_id, attribute_id, is_required)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE is_required = VALUES(is_required)
                """, (template_id, aid, is_req))

# 创建新设备模板
def create_template(name, device_type, version=1, is_locked=0):
    sql = """INSERT INTO device_template (name, device_type, version, is_locked)
             VALUES (%s, %s, %s, %s)"""  # SQL插入语句用于创建新设备模板
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute(sql, (name, device_type, int(version or 1), int(is_locked or 0)))  # 执行插入操作
            return cur.lastrowid  # 返回新插入记录的ID

# 根据模板ID删除指定设备模板
def delete_template(template_id):
    # 若存在外键引用（device、template_attribute 等），可能会失败，这是合理保护
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute("DELETE FROM device_template WHERE id=%s", (template_id,))  # 执行删除操作

# 根据模板ID获取指定设备模板的详细信息
def get_template(template_id):
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute("SELECT * FROM device_template WHERE id=%s", (template_id,))  # 执行查询以获取指定设备模板
            return cur.fetchone()  # 返回查询结果

# 更新现有设备模板的信息
def update_template(template_id, name, device_type, version, is_locked):
    sql = """UPDATE device_template
             SET name=%s, device_type=%s, version=%s, is_locked=%s
             WHERE id=%s"""  # SQL更新语句用于更新设备模板信息
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute(sql, (name, device_type, int(version or 1), int(is_locked or 0), template_id))  # 执行更新操作
