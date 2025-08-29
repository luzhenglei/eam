# services/attribute_service.py
from db import get_conn  # 引入数据库连接函数

# 允许的属性作用域列表
ALLOWED_SCOPES = ["device", "port"]
# 允许的数据类型列表
ALLOWED_DTYPES = ["text", "int", "decimal", "bool", "date", "enum", "json"]

# 列出所有属性，根据提供的作用域过滤
def list_attributes(scope=None):
    sql = "SELECT id, code, name, scope, data_type, allow_multi FROM attribute_def"  # 基础SQL查询语句
    args = []  # 参数列表
    if scope in ALLOWED_SCOPES:  # 如果提供的作用域在允许的作用域列表中
        sql += " WHERE scope=%s"  # 添加作用域过滤条件
        args.append(scope)  # 将作用域添加到参数列表
    sql += " ORDER BY id DESC"  # 按ID降序排列结果
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute(sql, args)  # 执行查询
            return cur.fetchall()  # 返回查询结果

# 分页列出属性，根据查询字符串、作用域和数据类型进行过滤
def list_attributes_paged(q="", scope="", data_type="", page=1, page_size=10):
    where = []  # 用于存储WHERE子句中的条件
    args = []  # 参数列表
    if q:  # 如果提供了查询字符串
        where.append("(code LIKE %s OR name LIKE %s)")  # 添加模糊查询条件
        args += [f"%{q}%", f"%{q}%"]  # 将查询字符串添加到参数列表，前后加%以便进行模糊匹配
    if scope in ALLOWED_SCOPES:  # 如果提供的作用域在允许的作用域列表中
        where.append("scope = %s")  # 添加作用域过滤条件
        args.append(scope)  # 将作用域添加到参数列表
    if data_type in ALLOWED_DTYPES:  # 如果提供的数据类型在允许的数据类型列表中
        where.append("data_type = %s")  # 添加数据类型过滤条件
        args.append(data_type)  # 将数据类型添加到参数列表

    base = "FROM attribute_def"  # 基础FROM子句
    if where:  # 如果WHERE子句中有条件
        base += " WHERE " + " AND ".join(where)  # 连接所有条件并添加到基础FROM子句

    # 获取总记录数
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute(f"SELECT COUNT(*) AS c {base}", args)  # 执行查询
            total = cur.fetchone()["c"]  # 获取总记录数

            # 获取分页数据
            offset = max(0, (page-1)*page_size)  # 计算分页偏移量
            cur.execute(f"SELECT id, code, name, scope, data_type, unit, min_value, max_value, allow_multi, description "
                        f"{base} ORDER BY id DESC LIMIT %s OFFSET %s",  # 构建分页查询语句
                        args + [page_size, offset])  # 添加分页参数
            rows = cur.fetchall()  # 获取查询结果
            return rows, total  # 返回查询结果和总记录数

# 根据属性ID获取单个属性的详细信息
def get_attribute(attr_id):
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute("SELECT * FROM attribute_def WHERE id=%s", (attr_id,))  # 执行查询
            return cur.fetchone()  # 返回查询结果

# 创建新属性
def create_attribute(code, name, scope, data_type, unit, min_value, max_value, allow_multi, description):
    sql = """INSERT INTO attribute_def
             (code, name, scope, data_type, unit, min_value, max_value, allow_multi, description)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""  # SQL插入语句
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute(sql, (code, name, scope, data_type, unit, min_value, max_value, allow_multi, description))  # 执行插入操作
            return cur.lastrowid  # 返回新插入记录的ID

# 更新现有属性的信息
def update_attribute(attr_id, code, name, scope, data_type, unit, min_value, max_value, allow_multi, description):
    sql = """UPDATE attribute_def SET
             code=%s, name=%s, scope=%s, data_type=%s,
             unit=%s, min_value=%s, max_value=%s, allow_multi=%s, description=%s
             WHERE id=%s"""  # SQL更新语句
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute(sql, (code, name, scope, data_type, unit, min_value, max_value, allow_multi, description, attr_id))  # 执行更新操作

# 根据属性ID删除属性
def delete_attribute(attr_id):
    with get_conn() as conn:  # 获取数据库连接
        with conn.cursor() as cur:  # 创建游标对象
            cur.execute("DELETE FROM attribute_def WHERE id=%s", (attr_id,))  # 执行删除操作
