# services/option_service.py
from db import get_conn

# 作为“属性本体”的代理根节点的固定 code
ROOT_CODE = "__root__"

def list_options(attribute_id):
    sql = """SELECT id, attribute_id, name, code, parent_id, sort_order
             FROM attribute_option
             WHERE attribute_id=%s
             ORDER BY COALESCE(parent_id,0), sort_order, id"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (attribute_id,))
            return cur.fetchall()

def get_option(opt_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM attribute_option WHERE id=%s", (opt_id,))
            return cur.fetchone()

# ===== 根节点（作为“属性本体”的代理） =====
def get_root_option(attribute_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM attribute_option WHERE attribute_id=%s AND code=%s LIMIT 1",
                        (attribute_id, ROOT_CODE))
            return cur.fetchone()

def ensure_root_option(attribute_id, attr_name=None):
    root = get_root_option(attribute_id)
    if root:
        return root
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO attribute_option (attribute_id, name, code, parent_id, sort_order)
                           VALUES (%s, %s, %s, %s, %s)""",
                        (attribute_id, attr_name or f"属性{attribute_id}", ROOT_CODE, None, 0))
            cur.execute("SELECT * FROM attribute_option WHERE id=LAST_INSERT_ID()")
            return cur.fetchone()

def _ensure_parent_same_attribute(parent_id, attribute_id):
    if not parent_id:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT attribute_id FROM attribute_option WHERE id=%s", (parent_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("父节点不存在")
            if row["attribute_id"] != attribute_id:
                raise ValueError("父节点不属于同一属性，禁止跨属性挂载")

def create_option(attribute_id, name, code=None, parent_id=None, sort_order=0):
    # 未选择父节点时，默认挂到根
    root = ensure_root_option(attribute_id)
    parent_id = parent_id or root["id"]
    _ensure_parent_same_attribute(parent_id, attribute_id)
    sql = """INSERT INTO attribute_option (attribute_id, name, code, parent_id, sort_order)
             VALUES (%s, %s, %s, %s, %s)"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (attribute_id, name, code, parent_id, sort_order))
            return cur.lastrowid

def update_option(opt_id, name, code, parent_id, sort_order):
    # 查出该选项的属性
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT attribute_id FROM attribute_option WHERE id=%s", (opt_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError("选项不存在")
            attribute_id = row["attribute_id"]

    # 未选择父节点时，默认挂到根
    root = ensure_root_option(attribute_id)
    parent_id = parent_id or root["id"]
    _ensure_parent_same_attribute(parent_id, attribute_id)

    sql = """UPDATE attribute_option
             SET name=%s, code=%s, parent_id=%s, sort_order=%s
             WHERE id=%s"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (name, code, parent_id, sort_order, opt_id))

def delete_option(opt_id):
    # 注意：ON DELETE CASCADE 会删除子树，谨慎
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM attribute_option WHERE id=%s", (opt_id,))

def list_children(attribute_id, parent_id=None):
    from db import get_conn
    root = ensure_root_option(attribute_id)
    pid = parent_id if parent_id is not None else root["id"]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, code, parent_id, sort_order
                FROM attribute_option
                WHERE attribute_id=%s AND parent_id=%s
                ORDER BY sort_order, id
            """, (attribute_id, pid))
            return cur.fetchall()

def get_option_chain(option_id):
    """
    返回从第一层开始到当前 option 的“祖先链”（不含 root 本身）。
    例如： [层1选项, 层2选项, ..., 目标选项]
    """
    chain = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT ao.id, ao.name, ao.code, ao.parent_id, ao.attribute_id
                           FROM attribute_option ao WHERE ao.id=%s""", (option_id,))
            row = cur.fetchone()
            if not row:
                return []
            attribute_id = row["attribute_id"]
            root = get_root_option(attribute_id)

            cur_row = row
            while cur_row:
                chain.append({"id": cur_row["id"], "name": cur_row["name"], "parent_id": cur_row["parent_id"]})
                pid = cur_row["parent_id"]
                if not pid or (root and pid == root["id"]):
                    break
                cur.execute("SELECT id, name, code, parent_id FROM attribute_option WHERE id=%s", (pid,))
                cur_row = cur.fetchone()
    chain.reverse()
    return chain
