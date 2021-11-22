
import ast, astunparse

import os
from loguru import logger
import easydict
import hashlib

import queue
import time, random
import base64, json, traceback

import mysql.connector as my

# 用于实现图形化建模任务执行状态更新到表中的数据库操作


# #  mysql -h172.18.233.159 -P8066 -uiasp_119 -p"iasp_119" --default-character-set=utf8 iasp_dev_119
# select comp_id, comp_name, comp_desc from c_component where parent_comp_id='000000';
'''
+---------+--------------------------+--------------------------+
| comp_id | comp_name                | comp_desc                |
+---------+--------------------------+--------------------------+
| 010000  | 数据输入和输出           |                          |   datasource
| 020000  | 数据转换                 |                          |  preprocess
| 030000  | 特征选择                 |                          |
| 040000  | 统计函数                 |                          |
| 050000  | 回归                     |                          |
| 060000  | 群集                     |                          |
| 070000  | 分类                     | 机器学习分类算法         |        cls_algo
| 080000  | 模型训练                 |                          |      train
| 090000  | 模型评分和评估           |                          |        evalu
| 100000  | Python 语言              |                          |
| 110000  | R 语言                   |                          |
| 120000  | 文本分析                 |                          |
| 130000  | 计算机视觉               |                          |
| 140000  | 建议                     |                          |
| 150000  | 异常检测                 |                          |
| 160000  | 测试模块-titanic         | 测试模块                 |
| 170000  | aiops算法模型            | aiops算法模型            |
| 180000  | 机器学习分类算法         | 机器学习分类算法         |
+---------+--------------------------+--------------------------+

comp_id 使用 class_name hashcode 截取
'''

cata_code = {
    "datasource":"010000",
    "preprocess": "020000",
    "cls_algo": "070000",
    "train": "080000",
    "evalu": "090000",
}

class DBPool:
    def __init__(self, conn_param, max_conn=10, max_idle_min=10):
        self.conn_param = { k: conn_param.get(k, None) for k in ['user', 'password','host', 'port', 'database'] }
        self.dbqueue = queue.LifoQueue()
        self.dbs = queue.Queue()
        self.max_conn = max_conn
        self.max_idle = max_idle_min*60  # 最多闲置 10 分钟

    def __call__(self, **kwargs):
        return my.connect(** self.conn_param)
    def get(self):
        try:
            for i in range(3):
                xx = self.dbqueue.get_nowait()
                db, tm = xx
                if time.time() - tm > self.max_idle:
                    try:
                        db.close() # evict the connection
                    except:
                        pass
                    self.dbs.get()
                else:
                    return db
        except:
            if self.dbs.qsize() < self.max_conn:
                db = my.connect(**self.conn_param, autocommit=False)
                db.autocommit = False
                self.dbs.put(1)
                return db
            else:
                return None
    def ret(self, db):
        self.dbqueue.put([db, time.time()])

db_pool = None
def pool_init(conn_param, max_conn = 1):
    global db_pool
    if db_pool is not None:
        return
    if type(conn_param) == str:
        conn_param = json.loads(base64.decodebytes(conn_param.encode('utf8')))
    db_pool = DBPool(conn_param, max_conn=max_conn)
    random.seed(time.time())

def dbfunc(func):
    def inner(*args, **kwargs):
        db = db_pool.get()
        # logger.debug("dbfunc1 got a connection {} {}", db, traceback.format_exc())
        cur = db.cursor()
        try:
            return func(db, cur, *args, **kwargs)
        except Exception as x:
            logger.error("db oper failed {} {}", x, traceback.format_exc())
            db.rollback()
            return None
        finally:
            cur.close()
            db.commit()
            db_pool.ret(db)
    return inner


@dbfunc
def insert_db(db, cur, catalog, comp_name, desc, inparams, outparams):
    parent_comp_id = cata_code[catalog]
    comp_id = hashlib.sha1(comp_name.encode('utf8')).hexdigest()[-9:]
    print('---import', parent_comp_id, comp_id, comp_name, desc)

    # if True:
    #     return

    cur.execute("select comp_id, parent_comp_id from c_component where comp_id=%s and parent_comp_id='000000'", [comp_id ,])
    if len(cur.fetchall()) > 0:
        print("invalid comp_id, it is a catalog")
        return
    cur.execute("delete from c_component where comp_id=%s", [comp_id ,])
    cur.execute("delete from c_comp_prop where comp_id=%s", [comp_id ,])
    cur.execute("delete from c_comp_point where comp_id=%s", [comp_id ,])

    sql = '''INSERT INTO c_component(COMP_ID, COMP_NAME, COMP_DESC, PARENT_COMP_ID, CODE_FILE_DIR, PACKAGE_INFO, 
    ICON, IS_LEAF, IS_PUBLIC, IS_VALID, CREATE_ID, CREATE_TIME, OP_ID, OP_TIME, OP_NOTE) 
    VALUES (%s, %s, %s, %s, NULL, NULL, 'null', 'Y', 'Y', 'Y', 'system', now(), 'system', now(), 'mods2db')'''
    cur.execute(sql, [comp_id, desc, comp_name, parent_comp_id])

    for i, p in enumerate(inparams):
        sql = '''INSERT INTO c_comp_prop(COMP_ID, PROP_KEY, PROP_NAME_ZH, PROP_NAME_EN, PROP_DESC_ZH, PROP_DESC_EN, 
    INPUT_TYPE, POINT_ID, POINT_VALUE_TYPE, DEAULT_VALUE, IS_REQUIRED, IS_VALID, 
    CREATE_ID, CREATE_TIME, OP_ID, OP_TIME, OP_NOTE,
     ext_type)
     VALUES (%s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s, 'Y', 'Y', 'iasp_dev', now(), 'iasp_dev', now(), 'mods2db', 
      %s)'''
        cur.execute(sql, [comp_id, p['name'], p['name'], p['name'], p['desc'], p['desc'],
                          'input', f"IN_{ i +1}", p.get('type','val'), p.get('defval', ''),
                          p.get('ext_type', None)
                          ])
        if p.get('linkable', False):
            sql = '''INSERT INTO c_comp_point(COMP_ID, POINT_ID, EXT_TYPE, POINT_DESC, IS_VALID, 
            CREATE_ID, CREATE_TIME, OP_ID, OP_TIME, OP_NOTE) 
            VALUES (%s, %s, %s, %s, 'Y', 
            'iasp_dev', now(), 'iasp_dev', now(), 'mods2db')'''
            cur.execute(sql, [comp_id, f"IN_{ i +1}", p.get('ext_type', None), p['name']])

    for i, p in enumerate(outparams):
        sql = '''INSERT INTO c_comp_prop(COMP_ID, PROP_KEY, PROP_NAME_ZH, PROP_NAME_EN, PROP_DESC_ZH, PROP_DESC_EN, 
    INPUT_TYPE, POINT_ID, POINT_VALUE_TYPE, DEAULT_VALUE, IS_REQUIRED, IS_VALID, 
    CREATE_ID, CREATE_TIME, OP_ID, OP_TIME, OP_NOTE,
    ext_type)
     VALUES (%s, %s, %s, %s, %s, %s,
      %s, %s, %s, %s, 'Y', 'Y', 'iasp_dev', now(), 'iasp_dev', now(), 'mods2db', 
      %s)'''
        cur.execute(sql, [comp_id, p['name'], p['name'], p['name'], p['desc'], p['desc'],
                          'output', f"OUT_{ i +1}", p.get('type','val'), p.get('defval', ''),
                          p.get('ext_type', None)
                          ])
        if p.get('linkable', False):
            sql = '''INSERT INTO c_comp_point(COMP_ID, POINT_ID, EXT_TYPE, POINT_DESC, IS_VALID, 
            CREATE_ID, CREATE_TIME, OP_ID, OP_TIME, OP_NOTE) 
            VALUES (%s, %s, %s, %s, 'Y', 
            'iasp_dev', now(), 'iasp_dev', now(), 'mods2db')'''
            cur.execute(sql, [comp_id, f"OUT_{ i +1}", p.get('ext_type', None), p['name']])


def parse_one_class(a: ast.ClassDef):
    assigns = {}
    funcs = {}
    for e in a.body:
        if type(e) == ast.Assign:
            assigns[e.targets[0].id] = eval(astunparse.unparse(e.value))
        elif type(e) == ast.FunctionDef:
            e1: ast.FunctionDef = e
            #print("func", e1.name)
            funcs[e1.name] = 1

    if 'fit' not in funcs and 'transform' not in funcs:
        return None
    if 'inparams' not in assigns and 'outparams' not in assigns and 'catalog' not in assigns:
        return None

    # print("inparams", assigns['inparams'])
    # print("outparams", assigns['outparams'])
    # print("catalog", assigns['catalog'])
    return assigns

def parse_one_file(pkg, fpath):

    with open(fpath, 'r', encoding='utf8') as fi:
        src = fi.read()

    models = []
    a = ast.parse(src)
    for e in a.body:
        if type(e) == ast.ClassDef:
            cls_name = e.name
            mod = parse_one_class(e)
            if mod is None:
                continue
            mod['name'] = cls_name
            mod['pkg'] = pkg
            mod['type_'] = f"{mod['catalog']}.{cls_name}"
            models.append(easydict.EasyDict(mod))
        else:
            # print(type(e), e)
            pass
    return models

def parse_all(basepath = 'd:/worksrc/git/iasp-pymod/guimod'):
    models = []
    for dirpath, dirnames, filenames in os.walk(basepath):
        for f in filenames:
            if f[:2] == '__': continue
            if f[-3:] != '.py': continue
            pkg = dirpath[len(basepath)+1:].replace('\\', '.') + "." + f[:-3]
            mo = parse_one_file(pkg, os.path.join(dirpath, f))
            models.extend(mo)
    # 需要检查下名称是否重复, 如果有重复要做告警
    names = set([i['name'] for i in models])
    if len(models) > len(names):
        logger.warning("model name duplicated: ")
        pkgs = {n:[] for n  in names}
        for m in models:
            pkgs[m['name']].append(m['pkg'])
        for n, pkg in pkgs.items():
            if len(pkg) > 1:
                logger.warning('--- {} {}', n, pkg)
    return models


def load2db(basepath = 'd:/worksrc/git/iasp-pymod/guimod'):
    dbconf = {"user": "iasp_119", "password": "iasp_119", "host": "172.18.233.159", "port": 8066,
              "database": "iasp_dev_119"}
    pool_init(dbconf, max_conn=1)
    models = parse_all(basepath)
    for m in models:
        comp_name = f"{m.pkg}.{m.name}"
        print(comp_name, m.catalog, len(m.inparams), len(m.outparams), m.desc)
        insert_db(m.catalog, comp_name, m.desc, m.inparams, m.outparams)

if __name__ == '__main__':
    load2db()
    # parse_all()
