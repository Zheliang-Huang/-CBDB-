#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import json
import time
import sqlite3
import asyncio
import logging
import traceback
import platform
import logging.handlers

from concurrent.futures import ThreadPoolExecutor

import tornado.web
import tornado.httpserver
import tornado.ioloop
from tornado.ioloop import PeriodicCallback

# =========================================================
# 日志配置
# =========================================================

log_path = '/data/python_logs/'

if not os.path.exists(log_path):
    os.makedirs(log_path)

logger = logging.getLogger()

logger.setLevel(logging.INFO)

log_file = logging.handlers.TimedRotatingFileHandler(
    os.path.join(log_path, 'apiknowledgegraph-.log'),
    when='midnight',
    interval=1,
    backupCount=30
)

log_file.suffix = '%Y-%m-%d.log'

formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'
)

log_file.setFormatter(formatter)

logger.addHandler(log_file)

# =========================================================
# SQLite 配置
# =========================================================

os_name = platform.system()

if os_name == "Windows":
    sql_path = r'H:\知识图谱\latest.db'
else:
    sql_path = '/data/latest.db'

conn = None

# 线程池
executor = ThreadPoolExecutor(
    max_workers=8
)


def init_db():
    """
    初始化 SQLite
    """

    global conn

    conn = sqlite3.connect(
        sql_path,
        check_same_thread=False,
        timeout=30
    )

    # WAL 模式
    conn.execute("PRAGMA journal_mode=WAL;")

    # 提高性能
    conn.execute("PRAGMA synchronous=NORMAL;")

    # cache
    conn.execute("PRAGMA cache_size=100000;")

    # 临时表放内存
    conn.execute("PRAGMA temp_store=MEMORY;")

    # mmap
    conn.execute("PRAGMA mmap_size=30000000000;")

    # 数据库预热
    conn.execute("SELECT 1")

    # 预加载 schema
    conn.execute("""
        SELECT c_personid
    FROM BIOG_MAIN
    LIMIT 200000
    """)

    conn.commit()

    logger.info("SQLite 初始化完成")


def warmup_sqlite_file():

    logger.info("开始预热 SQLite 文件")

    with open(sql_path, 'rb') as f:

        while f.read(1024 * 1024):
            pass

    logger.info("SQLite 文件预热完成")
    

def keep_db_hot():

    try:

        with open(sql_path, 'rb') as f:

            # 每次读前 32MB
            f.read(32 * 1024 * 1024)

        logger.info("数据库文件保活成功")

    except Exception as e:

        logger.error(e)

# =========================================================
# 工具函数
# =========================================================

def set_default_header(handler):

    handler.set_header(
        'Access-Control-Allow-Origin',
        '*'
    )

    handler.set_header(
        'Access-Control-Allow-Headers',
        'x-requested-with'
    )

    handler.set_header(
        'Access-Control-Allow-Methods',
        'POST, GET, PUT, DELETE'
    )


def return_error(error_num, error_msg):

    resp_dic = {
        'code': error_num,
        'msg': f'错误：{error_msg}'
    }

    logger.error(error_msg)

    logger.error(traceback.format_exc())

    return json.dumps(
        resp_dic,
        ensure_ascii=False
    )


# =========================================================
# QueryName 逻辑
# =========================================================


def query_name_logic(name_chn):

    starttime = time.time()

    cursor = conn.cursor()

    # 参数化 SQL（防注入）
    cursor.execute("""
                   SELECT
                	BIOG_MAIN.c_personid,
                	BIOG_MAIN.c_name_chn,
                	DYNASTIES.c_dynasty_chn,
                	BIOG_MAIN.c_name_chn_simp 
                FROM
                	BIOG_MAIN_FTS
                	LEFT JOIN BIOG_MAIN ON BIOG_MAIN.c_personid = BIOG_MAIN_FTS.c_personid
                	LEFT JOIN DYNASTIES ON BIOG_MAIN.c_dy = DYNASTIES.c_dy 
                WHERE
                	BIOG_MAIN_FTS MATCH ? 
                	LIMIT 50
                   """, (name_chn,))

    name_list_raw = cursor.fetchall()

    logger.info(
        f'queryName 查询耗时: '
        f'{time.time() - starttime:.2f}秒'
    )

    resp_list = []

    # =====================================================
    # 一次性查别名（消灭 N+1）
    # =====================================================

    person_ids = [x[0] for x in name_list_raw]

    altname_map = {}

    if person_ids:

        placeholders = ",".join(
            ["?"] * len(person_ids)
        )

        cursor.execute(f"""
            SELECT
                ALTNAME_DATA.c_personid,
                ALTNAME_CODES.c_name_type_desc_chn,
                ALTNAME_DATA.c_alt_name_chn
            FROM ALTNAME_DATA
            LEFT JOIN ALTNAME_CODES
                ON ALTNAME_CODES.c_name_type_code =
                   ALTNAME_DATA.c_alt_name_type_code
            WHERE ALTNAME_DATA.c_personid
            IN ({placeholders})
        """, person_ids)

        alt_rows = cursor.fetchall()

        for pid, desc, altname in alt_rows:

            if pid not in altname_map:
                altname_map[pid] = []

            if desc not in ['未詳', '别名', '别名、曾用名']:
                altname_map[pid].append(
                    f"{desc}{altname}"
                )
            else:
                altname_map[pid].append(altname)

    # =====================================================
    # 返回数据
    # =====================================================

    for row in name_list_raw:

        pid = row[0]

        resp_list.append({
            'pid': pid,
            'name': row[1],
            'dynasty': row[2],
            'altname': '；'.join(altname_map.get(pid, [])
            )
        })

    resp_dic = {
        'code': 200,
        'msg': '操作成功',
        'query_num': len(resp_list),
        'data': resp_list
    }

    return json.dumps(
        resp_dic,
        ensure_ascii=False
    )


# =========================================================
# QueryInfo 逻辑
# =========================================================


def query_info_logic(json_data):

    cursor = conn.cursor()

    resp_dic = {}

    res_list = []

    pid = json_data.get('personId')

    if not pid:

        return return_error(
            500,
            '人物id不能为空'
        )

    # =====================================================
    # 获取人物名称
    # =====================================================

    cursor.execute("""
        SELECT c_name_chn
        FROM BIOG_MAIN
        WHERE c_personid = ?
    """, (pid,))

    person_row = cursor.fetchone()

    if not person_row:

        return return_error(
            500,
            '人物不存在'
        )

    person_name = person_row[0]
    
    # =====================================================
    # 亲属关系
    # =====================================================

    kin_rels = json_data.get('kin_rel')
    
    if kin_rels is not None:
        kin_rel_dic = {
            'elder': ['八世叔伯祖', '八世祖', '太高祖', '本生祖', '表姑', '表叔伯', '伯', '伯父', '伯叔母', '伯叔祖母', '伯叔祖母姪', '伯祖', '伯祖父', '曾伯叔祖', '從曾祖', '曾祖', '曾祖姑', '曾祖母', '曾祖母之父', '曾祖母姪', '曾祖之從兄弟', '再從曾祖', '從父', '伯叔父', '從祖', '伯叔祖', '得許娶其女', '得許娶其孫女', '嫡母', '第二任妻父', '第二任妻之叔伯', '第二任妻之祖', '第三任妻父', '第三任妻之祖', '第四任妻父', '第五任妻父', '第一任妻父', '第一任妻之叔伯', '第一任妻之直系祖先', '第一任妻之祖', '二十七世祖', '二十三世祖', '二十世祖', '二十四世祖', '二十一世祖', '非親生父', '非親生母', '夫之曾祖', '夫之從父', '夫之祖父', '夫之祖母', '父', '父之外祖', '高祖', '四世祖', '高祖母', '姑父', '姑母', '許妻以女', '許妻以孫女', '繼父', '繼母', '繼祖', '九世叔伯祖', '九世祖', '太高曾祖', '舅公', '父或母之舅', '舅婆', '父或母之舅母 FMBW/MMBW', '六世祖', '太祖', '母', '母舅', '七世伯叔祖', '七世祖', '太曾祖', '妻舅祖', '妻之曾叔伯祖', '妻之曾祖', '妻之高祖', '妻之姑母', '妻之母舅', '妻之七世祖', '妻之十七世祖', '妻之叔伯', '妻之叔伯祖', '妻之同族祖先', '妻之五世祖', '妻之直系祖先', '妻之祖父', '妻子的祖母', '前母', '妾之父', '三十八世祖', '三十六世祖', '三十世祖', '十八世祖', '十二世祖', '十九世祖', '十六世祖', '十七世祖', '十三世祖', '十世祖', '十四世祖', '十五世祖', '十一世祖', '始祖', '始遷祖', '叔', '叔父', '叔祖', '叔祖父', '庶母', '私生子女之本生母', '私生子之本生父', '四十二世祖', '四十六世祖', '四十七世祖', '四十三世祖', '四十四世祖', '四十五世祖', '嗣父', '嗣母', '嗣祖父', '嗣祖母', '太伯叔祖', '六世叔伯祖', '堂伯叔母', '同族長輩', '同族六世祖', '同族四世祖', '同族五世祖', '外曾祖父', '外高祖父', '外祖父', '外祖父之兄弟', '外祖母', '翁', '公公', '丈夫之父', '五十六世祖', '五十七世祖', '五十三世祖', '五十世祖', '五十四世祖', '五十五世祖', '五世叔伯祖', '五世祖', '高曾祖', '養父', '養母', '姨', '從母', '姨父', '岳父', '岳母', '岳母之父', '再從父', '丈夫之母', '婆婆', '直系祖先', '姪女未婚夫之父', '族曾祖', '族父', '族姑', '族祖父', '族父的父親, 即祖父的堂兄弟', '族祖姑', '祖父', '祖父之從兄弟', '再從祖', '祖姑', '祖母', '祖母姪'],
            'same_gen': ['表弟', '表姐', '表姐妹', '表姐妹婿', '表妹', '表兄', '表兄弟', '長兄', '從弟', '從妹', '從兄', '從兄弟', '堂兄弟', '從姊', '從姊妹', '弟', '第二任妻', '第二任妻兄', '第二任妻兄弟', '第二任妻之堂兄弟', '第二任丈夫', '第三任妻', '第三任丈夫', '第四任妻', '第五任妻', '第一任妻', '第一任妻之堂兄弟', '第一任丈夫', '兒子未婚妻之伯叔父', '夫之姐妹', '夫之兄弟', '姑表弟', '姑表兄', '姑表兄弟', '季弟', '繼弟', '繼妹', '繼兄', '繼兄弟', '繼姊妹', '姐夫', '舅表弟', '舅表兄', '舅表兄弟', '連襟', '妻姊妹之夫', '妹', '妹夫', '內兄弟', '妻兄弟', '妻弟', '妻妹', '妻兄', '妻之表兄弟姊妹', '妻之堂兄弟', '妻之姊妹', '妻之族兄弟姊妹', '妻子', '妻姊', '妾', '妾之叔伯', '妾之兄弟', '堂舅', '母亲的叔伯兄弟', '堂姊妹之夫', '同母異父弟', '同母異父兄', '同母異父兄弟', '未婚夫', '未婚妻', '兄', '兄弟', '兄弟之妻', '姨表弟', '姨表兄', '姨表兄弟', '姨表姊妹', '從母妹', '姨表姊妹之夫', '岳母之外甥', '妻之姨表兄弟', '再從弟', '再從兄', '再從兄弟', '丈夫', '姊', '姊妹', '姊妹夫', '族弟', '族妹', '族兄', '族兄弟', '族姊', '族姊妹', '族姊妹之夫'],
            'younger': ['子', '八女', '八女婿', '八世孫', '雲孫', '八子', '表姪', '曾孫', '重孫', '曾孫女', '曾孫女婿', '曾孫女之子', '曾孫媳婦', '曾姪孫', '從曾孫', '曾姪孫女', '曾姪孫女婿', '長女', '長女婿', '長子', '第一子', '次子', '從來孫', '從女', '姪女', '從七世孫', '從子', '姪子', '弟之外孫', '第二任女婿', '第一任女婿', '獨女', '獨子', '二女', '二女婿', '二十二子', '二十七世孫', '二十三世孫', '二十三子', '二十世孫', '二十四世孫', '二十四子', '二十五子', '二十一世孫', '二十一子', '二十子', '非親生女', '非親生子', '夫堂姪', '夫之從女', '夫之從子', '夫之姪孫', '夫之從孫', '姑父之姪孫', '姑父之從孫', '姑母之曾孫', '姑表兄弟之孫', '姑母之孫', '姑表兄弟之子', '過繼的嗣孫', '過繼的嗣子', '季子', '繼女', '繼孫子', '繼子', '九女', '九女婿', '九世孫', '九子', '六女', '六女婿', '六世從孫', '姪昆孫', '六世孫', '昆孫', '六子', '女兒', '女婿', '七女', '七女婿', '七世孫', '仍孫', '七世孫婿', '七子', '妻之從女', '妻之從子', '妻之甥女婿', '妻之外甥', '妻之外甥女', '妻之姨夫', '妾之女', '妾之子', '三女', '三女婿', '三十八世孫', '三十六世孫', '三十世孫', '三子', '甥女', '甥女婿', '姊妹之女婿', '甥孫', '十八世孫', '十八子', '十二女', '十二世孫', '十二子', '十九世孫', '十九子', '十六世孫', '十六子', '十女', '十七世孫', '十七世孫婿', '十七子', '十三女', '十三世孫', '十三子', '十世孫', '十四女', '十四世孫', '十四子', '十五世孫', '十五子', '十一女', '十一世孫', '十一子', '十子', '庶子', '私生孫', '私生子', '四女', '四女婿', '四十二世孫', '四十六世孫', '四十七世孫', '四十三世孫', '四十五世孫', '四子', '嗣子(作為繼承人的兒子)', '孫', '孫女', '孫女婿', '孫媳', '堂姊妹之子', '同族六世孫', '同族四世孫', '同族晚輩', '同族晚輩之夫', '同族五世孫', '外甥', '外甥孫婿', '外孫', '外孫女', '外孫女婿', '外孫女之子', '外孫之子', '外玄孫', '唯一幸存的兒子', '五女', '五女婿', '五十六世孫', '五十七世孫', '五十三世孫', '五十四世孫', '五十五世孫', '五世孫', '來孫', '五世孫婿', '五子', '幸存的長子', '兄弟之八世孫', '兄弟之九世孫', '玄孫', '四世孫', '玄孫女', '玄孫女婿', '養女', '養子(非嗣子)', '再從曾孫', '堂姪曾孫', '再從孫', '堂姪孫', '再從子', '堂姪', '直系後裔', '直系後裔之夫', '姪女之夫', '姪孫', '從孫', '姪孫女', '姪孫女之夫', '姪媳婦', '子婦', '兒媳', '族曾孫', '族女', '族孫', '族子'],
            'unknown': ['表親', '非可用', '後裔(自稱)', '未詳', '姻親', '自謂...之後', '族人(輩分不詳)']
            }
        kin_dic = {}
        if len(kin_rels) != 0:
            kin_rel_list = []
            for kin in kin_rels:
                for kin_item in kin_rel_dic[kin]:
                    kin_rel_list.append(kin_item)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                	KIN_DATA.c_kin_id,
                	KINSHIP_CODES.c_kinrel_chn,
                	TEXT_CODES.c_title_chn,
                	KIN_DATA.c_pages 
                FROM
                	KIN_DATA
                	LEFT JOIN BIOG_MAIN ON BIOG_MAIN.c_personid = KIN_DATA.c_personid
                	LEFT JOIN KINSHIP_CODES ON KIN_DATA.c_kin_code = KINSHIP_CODES.c_kincode
                	LEFT JOIN TEXT_CODES ON KIN_DATA.c_source = TEXT_CODES.c_textid
                WHERE 
                	KIN_DATA.c_personid = \'{}\'
                   '''.format(pid))
            kin_list_raw = cursor.fetchall()
            kinname_map = {}
            kin_ids = [x[0] for x in kin_list_raw]
            if kin_ids:
                placeholders = ",".join(["?"] * len(kin_ids))
                
                cursor.execute(
                    f'SELECT c_personid , c_name_chn FROM BIOG_MAIN WHERE c_personid IN({placeholders})',kin_ids)
                
                kin_rows = cursor.fetchall()
                
                for pid, k_name in kin_rows:
                    if pid not in kinname_map:
                        kinname_map[pid] = k_name
                        
            # 返回数据 
            for kin_set in kin_list_raw:
                kin_dic_each = {}
                kin_rel_name = kin_set[1]
                if kin_rel_name in kin_rel_list:
                    kin_dic_each['entity1'] = person_name
                    kin_dic_each['entity1_info'] = ''
                    kin_dic_each['entity1_category'] = '人名'
                    kin_id = kin_set[0]
                    kin_name = kinname_map[kin_id]
                    kin_dic_each['entity2'] = str(kin_name)
                    source = ''
                    if kin_set[2] is not None:
                        source = kin_set[2]
                    if kin_set[3] is not None:
                        source = source+'(頁{})'.format(kin_set[3])
                    kin_dic_each['entity2_info'] = source
                    kin_dic_each['entity2_category'] = '亲属关系'
                    kin_dic_each['relationship'] = kin_rel_name
                    res_list.append(kin_dic_each)

    # =====================================================
    # 社会关系
    # =====================================================
    
    assoc_rels = json_data.get('assoc_rel')
    
    if assoc_rels is not None:
        assoc_rel_dic = {
            'positive': ['是Y的恩主', '恩主是Y', '黨羽為Y', '黨魁為Y', '友', '同年友', '推薦', '被Y推薦', '被Y欣賞/器重', '欣賞/器重', '為Y之門人', '門人為Y', '與Y結黨', '為Y之學生', '學生為Y', '辟', '為Y所辟', '得到Y的支持', '支持', '為Y所著書作序', '書序由Y所作', '為Y學派的成員', '該學派的成員為Y', '為Y之弟子', '弟子為Y', '為Y作哀辭 [併入167]', '哀辭由Y所作[併入166]', '其志業由Y傳承', '以Y之志業自任', '墓誌銘由Y所作', '為Y作墓誌銘', '書跋由Y所作', '為Y所著書作跋', '醫愈', '被Y醫愈', '從Y遊', '從遊者為Y', '職業被Y繼承', '繼承Y的職業', '被Y推薦參加制科考試', '推薦Y參加制科考試', '文風效法Y', '文風為Y所效法', '為Y之幕僚', '幕僚為Y', '節行為Y所稱道', '稱道Y之節行', '詩作為Y所稱道', '稱道Y的詩作', '為Y撰寫學術源宗', '學術源宗由Y撰寫', '被Y養育', '養育', '為Y作字說、名述', '字說、名述由Y所作', '遺奏推薦', '被Y在遺表中推薦', '論學', '相唱和', '同學、同門', '為Y之謀士', '以Y為謀士', '傳Y之學', '其學由Y所傳', '為Y之門客', '門客為Y', '傳記作者為Y', '為Y作傳', '讚揚Y之學', '其學得到Y之讚揚', '為Y作祭文', '祭文由Y所作', '為Y作祝詞', '祝詞由Y所作', '為Y之書、畫作跋', '書、畫由Y作跋', '為Y作世系碑記', '世系碑記由Y所作', '求Y為他人（第三方）作墓誌銘', '受Y的請求為他人（第三方）作墓誌銘', '為Y作廟碑記', '廟碑記由Y所作', '為Y作臨別贈言(送別詩、序)', '臨別得到Y所作贈言(送別詩、序)', '哀辭由Y所作', '為Y作哀辭', '稱道Y之文風', '文風為Y所稱道', '神道碑的書法由Y所作', '為Y之神道碑作書', '諡議由Y所作', '為Y作諡議', '對Y執弟子禮', '受Y之弟子禮', '出財贍給Y之軍', '軍隊得到Y之贍給', '義莊記由Y所作', '為Y作義莊記', '宗Y之學', '其學為Y所宗', '主Y家之教席', '聘Y主家之教席', '受經於Y', '傳經於Y', '為Y所葬', '葬', '喪事由Y經紀', '經紀Y之喪', '為Y作硯銘', '硯銘由Y所作', '從Y學', '教授Y', '年譜由Y所作', '為Y作年譜', '為Y作佛寺記', '佛寺記由Y所作', '神道碑額篆由Y所作', '為Y之神道碑作額篆', '為Y之義莊規矩作序', '義莊規矩序由Y所作', '合撰(編)著作', '為Y建立祠廟', 'Y為之建立祠廟', '私淑Y之學', '其學為Y所私淑', '教授Y之學', '其學由Y傳授學生', '擁立Y', '得到Y的擁立', '同會', '支持Y的政策 [併入27]', '其政策得到Y的支持[併入26]', '家譜由Y作序', '為Y之家譜作序', '與Y遊', '舉辦的鄉Ⅰ禮得到Y所作之序', '為Y舉辦的鄉Ⅰ禮作序', '以文章、學問受知於', '稱道Y之文章、學問', '齋、堂銘由Y所作', '為Y作齋、堂銘', '聘Y執教族學', '為Y所聘執教族學', '其後代出於Y之門', '門人為Y之後代', '從Y至貶所', '由Y陪伴至貶所', '碑陰為Y所作', '為Y作碑陰', '著述為Y所採', '採Y之著述', '墓誌銘由Y作跋', '為Y之墓誌銘作跋', '拜訪', '受到Y拜訪', '編輯Y之詩文', '詩文由Y編輯', '提議封贈Y', '以Y言得到封贈', '得到Y的喜爱', '喜爱', '研讀Y的著作', '其著作為Y所研讀', '禮待', '受Y之禮待', '政見趨同', '書風為Y所模仿', '模仿Y的書風', '向Y致賀', '從Y處收到賀詞 (occasion)', '家傳為Y所作', '為Y作家傳', '為Y作名說', '家譜由Y作序', '為Y家譜作序', '名說由Y所作', '戰友', '神道碑由Y所作', '為Y作神道碑', '受Y請求作神道碑', '向Y求神道碑', '墓表由Y所作', '為Y作墓表', '受Y請求作墓表', '向Y求墓表', '墓誌銘序由Y所作', '為Y墓誌銘作序', '神道碑序由Y所作', '為Y神道碑作序', '神道碑跋由Y所作', '為Y神道碑作跋', '墓表序由Y所作', '為Y墓表作序', '墓表跋由Y所作', '為Y墓表作跋', '挽詩、詞由Y所作', '為Y作挽詩、詞', '行狀由Y所作', '為Y作行狀', '畫贊(畫像記)由Y所作', '為Y作畫贊(畫像記)', '致書Y', '被致書由Y', '答Y書', '收到Y的答書', '贈詩、文', '收到Y的贈詩、文', '題Y的書帖', '其書帖由Y作題', '題Y之畫作', '題畫詩文的作者為Y', '為Y之建築物題詠、記、命名', '建築物得到Y的題詠、記、命名', '同道', '給Y錢物', '接受Y的錢物', '編輯Y的學術作品', '其學術作品由Y編輯', '學記（書院記）由Y所作', '為Y作學記（書院記）', '壙記由Y所作', '為Y作壙記', '為Y之收藏作跋', '收藏品由Y作跋', '向Y問學', '指教Y', '為Y之詩文作跋', '詩文跋由Y所作', '畫風師法Y', '畫風為Y所師法', '書、畫為Y所稱道', '稱道Y的書、畫', '為Y所作詩文作序', '詩文序由Y所作', '為Y篆匾額、器銘', '匾額、器銘由Y所篆', '為Y明冤', '其冤因Y得明', '奏錄Y之文', '其文為Y所奏錄', '代Y作文', '由Y代作文', '為Y所佑', '護佑Y', '為Y之潛邸舊人', '潛邸舊人為Y', '為Y之生祠作記', '其生祠由Y作記', '聘Y執教官學、書院', '為Y所聘執教官學、書院', '送別', '被送別', '贈畫', '得到贈畫', '獻文、書于Y', '收到Y所獻詩、文、書', '請Y作序、記', '受Y之請作序、記', '向Y借書', '借書給Y', '題Y之墓', '墓由Y題', '獻策於Y', '採納Y之獻策', '結義', '與Y共赴難', '為Y之行狀作跋', '行狀跋由Y所作', '為Y之家傳作跋', '家傳跋由Y所作', '幫助Y', '得到Y之幫助', '評論Y之著作', '著作被Y評論', '墓誌銘額篆由Y所作', '為Y的墓誌銘作額篆', '贈Y物', '受Y之贈物', '為Y取名、字', '名、字由Y所取', '祭文跋由Y所作', '為Y祭文作跋', '政績為Y所稱道', '稱道Y之政績', '刊刻Y之著述', '著述由Y刊刻', '求他人（第三方）為Y作墓誌銘', '墓誌銘係由Y向他人（第三方）求得', '求Y為他人（第三方）作神道碑', '受Y的請求為他人（第三方）作神道碑', '求他人（第三方）為Y作神道碑', '神道碑係由Y向他人（第三方）求得', '為Y之義女', '義女為Y', '為Y乞某事', '某事由Y所乞', '上奏論Y', '被Y奏論', '與Y旅遊', '為Y作道觀記', '道觀記由Y所作', '為Y作祠記', '祠記由Y所作', '世交父執為Y', '為Y之世交父執', '為Y主婚', 'Y為之主婚', '錄Y為假子(養子)', '為Y之假子(養子)', '校訂Y之著述', '著述由Y校訂', '獻詩集給Y', '被Y獻詩集', '家傳序由Y所作', '為Y之家傳作序', '為Y之書題詞', '書之題詞由Y所作', '軍事支持Y', '受Y之軍事支持', '推薦Y給他人（第三方）', '被Y推薦給他人（第三方）', '為Y請祀', 'Y為之請祀', '出財贍給Y', 'Y為之出財贍給', '為Y求謚號', 'Y為之求謚號', '與Y為講學', '為Y的墓誌銘書丹', '墓誌銘由Y書丹', '為Y作記', '記由Y所作', '為Y作去思碑/記', '去思碑/記由Y所作', '為Y作遺愛碑/記', '遺愛碑/記由Y所作', '為Y作德政碑/頌', '德政碑/頌由Y所作', '為y之挽詩 ,壽詩/文作序', '挽詩 ,壽詩/文序被Y所作', '陪同', '被Y陪同', '為Y書諱/填諱', '書諱/填諱由Y所作', '為Y的神道碑/墓誌/壙誌刊/刻石', '神道碑/墓誌/壙誌由Y刊/刻石'],
            'neutrality': ['研修理學', '為Y之部將', '部將為Y', '同鄉', '相識', '詩社成員', '同場屋/同應舉', '決定Y的科舉次第', '科舉次第由Y決定', '同僚', '副Y出使', '為使團副使Y的正使', '以宦官事Y', '為宦官Y所事', '為Y侍女', '以Y為侍女', '致Y啓', '收到Y的啓', '答Y啓', '收到Y的答啓', '向Y購買', '賣給Y', '佃戶為Y', '地主為Y', '家僕為Y', '家主為Y', '以醫術事Y', '被Y以醫術所事', '上司為Y', '下屬為Y', '為Y之考官', '考官為Y', '為Y之家僕', '家僕為Y', '為Y之婢', '婢為Y', '論政'],
            'negative': ['彈劾', '被Y彈劾', '反對/攻訐', '遭到Y的反對/攻訐', '排擠', '遭Y排擠', '籌劃謀殺', '被Y籌劃謀殺', '不合', '被Y以詩諷忤', '以詩諷忤Y', '忌/惡', '為Y所忌/惡', '拒絕在Y主政的政府中任職', '拒絕在Y的主政期出仕', '建議處決Y', '其處決係由Y建議', '反對/不支持Y的政策', '其政策被Y反對/不支持', '遭Y黨羽的攻訐', '其黨攻訐Y', '拒絕會面', '會面的邀請被Y拒絕', '被Y鞫治', '鞫治', '被Y逮捕', '逮捕', '根據Y的命令被處決', '下令處決', '攻訐[併入15]', '遭Y攻訐 [併入16]', '反對赦免', '對他的赦免遭到Y的反對', '處決', '被Y處決', '抵禦或討平叛軍Y', '叛軍遭到Y的抵禦或討平', '被Y（或追隨者）殺害', '其（或追隨者）殺害Y', '排Y之學', '其學為Y所排', '因與Y的交往受牽連', '與他的交往導致Y受牽連', '拒為Y掾屬', '欲辟Y為幕僚但被拒絕', '批評', '被Y批評', '被指為Y之同犯', '與Y爭權', '逃離Y的統治區', 'Y自其統治區中逃離', '拒為Y之黨', '邀Y入黨但遭到拒絕', '聯姻建議被Y拒絕', '拒絕Y的聯姻建議', '戰勝', '敗於', '降於', '接受Y之納降', '廢除Y的君位', '君位被Y廢除', '貸給Y', '向Y貸', '奪Y之妻', '妻被Y奪', '其同犯被指為Y', '陷害Y', '被Y陷害', '拒Y游説', '游説Y被拒絕', '蠱惑Y', '被Y蠱惑', '反對Y稱帝', '稱帝為Y所反對', '得罪Y', '被Y得罪', '與Y攀親戚敘族屬', 'Y與之攀親戚敘族屬', '軍事對抗Y', '被Y軍事對抗Y', '諂事Y', '被Y諂事', '原告為Y', '被告為Y', '反叛', '被Y反叛']
        }
        
        if len(assoc_rels) != 0:
            assoc_rel_list = []
            for assoc in assoc_rels:
                for assoc_item in assoc_rel_dic[assoc]:
                    assoc_rel_list.append(assoc_item)
            cursor = conn.cursor()
            cursor.execute('''
                    SELECT
                    	ASSOC_DATA.c_assoc_id,
                    	ASSOC_CODES.c_assoc_desc_chn,
                    	TEXT_CODES.c_title_chn,
                    	ASSOC_DATA.c_pages 
                    FROM
                    	ASSOC_DATA
                    	LEFT JOIN ASSOC_CODES ON ASSOC_CODES.c_assoc_code = ASSOC_DATA.c_assoc_code
                    	LEFT JOIN BIOG_MAIN ON BIOG_MAIN.c_personid = ASSOC_DATA.c_personid
                    	LEFT JOIN TEXT_CODES ON ASSOC_DATA.c_source = TEXT_CODES.c_textid 
                    WHERE
                    	ASSOC_DATA.c_personid = \'{}\'
                           '''.format(pid))
            assoc_list_raw = cursor.fetchall()
            for assoc_set in assoc_list_raw:
                assoc_dic_each = {}
                assoc_rel_name = assoc_set[1]
                if assoc_rel_name in assoc_rel_list:
                    assoc_dic_each['entity1'] = person_name
                    assoc_dic_each['entity1_info'] = ''
                    assoc_dic_each['entity1_category'] = '人名'
                    assoc_id = assoc_set[0]
                    cursor.execute(
                        'SELECT c_name_chn FROM BIOG_MAIN WHERE c_personid = {}'.format(assoc_id))
                    assoc_name_list = cursor.fetchall()
                    assoc_name = assoc_name_list[0][0]
                    assoc_dic_each['entity2'] = str(assoc_name)
                    source = ''
                    if assoc_set[2] is not None:
                        source = assoc_set[2]
                    if assoc_set[3] is not None:
                        source = source+'(頁{})'.format(assoc_set[3])
                    assoc_dic_each['entity2_info'] = source
                    assoc_dic_each['entity2_category'] = '社会关系'
                    assoc_dic_each['relationship'] = assoc_rel_name
                    res_list.append(assoc_dic_each)

    # =====================================================
    # 官职与社会区分信息
    # =====================================================

    offical_social = json_data.get('offical_social')

    if offical_social is not None:

        if 'official' in offical_social:
            cursor = conn.cursor()
            cursor.execute('''
                        SELECT
                        	APPOINTMENT_CODES.c_appt_desc_chn,
                        	OFFICE_CODES.c_office_chn,
                        	POSTED_TO_OFFICE_DATA.c_firstyear,
                        	POSTED_TO_OFFICE_DATA.c_lastyear,
                        	POSTED_TO_ADDR_DATA.c_addr_id,
                        	TEXT_CODES.c_title_chn,
                        	POSTED_TO_OFFICE_DATA.c_pages 
                        FROM
                        	POSTED_TO_OFFICE_DATA
                        	LEFT JOIN APPOINTMENT_CODES ON APPOINTMENT_CODES.c_appt_code = POSTED_TO_OFFICE_DATA.c_appt_code
                        	LEFT JOIN OFFICE_CODES ON OFFICE_CODES.c_office_id = POSTED_TO_OFFICE_DATA.c_office_id
                        	LEFT JOIN TEXT_CODES ON TEXT_CODES.c_textid = POSTED_TO_OFFICE_DATA.c_source
                        	LEFT JOIN POSTED_TO_ADDR_DATA ON POSTED_TO_ADDR_DATA.c_posting_id = POSTED_TO_OFFICE_DATA.c_posting_id
                        WHERE POSTED_TO_OFFICE_DATA.c_personid = \'{}\'
                           '''.format(pid))
            office_list_raw = cursor.fetchall()

            addr_name_map = {}
            c_addrs = [x[4] for x in office_list_raw]
            if c_addrs:
                placeholders = ",".join(["?"] * len(c_addrs))

                cursor.execute(
                    f'SELECT c_addr_id, c_name_chn FROM ADDR_CODES WHERE c_addr_id IN({placeholders})', c_addrs)

                c_addr_rows = cursor.fetchall()

                for addr_id, addr_name in c_addr_rows:
                    if addr_id not in addr_name_map:
                        addr_name_map[addr_id] = addr_name

            for office_set in office_list_raw:
                office_dic_each = {}
                office_dic_each['entity1'] = person_name
                office_dic_each['entity1_info'] = ''
                office_dic_each['entity1_category'] = '人名'
                office_dic_each['entity2'] = str(office_set[1])
                # office_dic_each['appointment'] = office_set[0]
                # office_dic_each['office_name'] = office_set[1]
                if office_set[2] != 0:
                    start_year = office_set[2]
                else:
                    start_year = '未知'
                if office_set[3] != 0:
                    end_year = office_set[3]
                else:
                    end_year = '未知'
                if office_set[4] is not None:
                    c_addr_id = office_set[4]
                else:
                    c_addr_id = '0'
                address = addr_name_map[c_addr_id]
                source = ''
                if office_set[5] is not None:
                    source = office_set[5]
                if office_set[6] is not None:
                    source = source+'(頁{})'.format(office_set[5])
                office_dic_each['entity2_info'] = '开始日期：{}；结束日期：{}；地区：{}；来源：{}。'.format(
                    start_year, end_year, address, source)
                office_dic_each['entity2_category'] = '官职'
                if office_set[0] is not None:
                    office_dic_each['relationship'] = office_set[0]
                else:
                    office_dic_each['relationship'] = '担任'
                res_list.append(office_dic_each)

        if 'social_diff' in offical_social:
            # 社会区分
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                	STATUS_CODES.c_status_desc_chn,
                	STATUS_TYPES.c_status_type_chn
                FROM
                	STATUS_DATA
                	LEFT JOIN STATUS_CODES ON STATUS_CODES.c_status_code = STATUS_DATA.c_status_code 
                	LEFT JOIN STATUS_CODE_TYPE_REL ON STATUS_DATA.c_status_code  = STATUS_CODE_TYPE_REL.c_status_code 
                	LEFT JOIN STATUS_TYPES ON STATUS_CODE_TYPE_REL.c_status_type_code = STATUS_TYPES.c_status_type_code 
                WHERE
                	STATUS_DATA.c_personid =\'{}\'
                '''.format(pid))
            social_diff_list_raw = cursor.fetchall()
            for social_set in social_diff_list_raw:
                social_dic = {}
                social_dic['entity1'] = person_name
                social_dic['entity1_info'] = ''
                social_dic['entity1_category'] = '人名'
                social_dic['entity2'] = str(social_set[0]).replace(
                    '[', '').replace(']', '')
                social_dic['entity2_info'] = ''
                social_dic['entity2_category'] = '社会区分'
                if '[未詳]' not in str(social_set[1]):
                    social_dic['relationship'] = str(social_set[1])
                else:
                    social_dic['relationship'] = '社会身份'
                res_list.append(social_dic)

    # =====================================================
    # 著述作品
    # =====================================================

    write_work = json_data.get('write_work')
    
    if write_work is not None:
        write_work_dic = {
            "annotate":     ['註疏者', '註釋者(含評點者)', '校對者'],
            "edit":         ['編輯者', '編輯助理'],
            "compilation":  ['編纂者', '收入Y集'],
            "create":       ['撰著者'],
            "publish":      ['出版者'],
            "translator":   ['翻譯者'],
            "other":        ['捐助者', '未詳']
        }
        # "annotate","edit","compilation","create","publish","translator","other"
        if len(write_work) != 0:
            write_work_list = []
            for write in write_work:
                for write_item in write_work_dic[write]:
                    write_work_list.append(write_item)
            cursor = conn.cursor()
            cursor.execute('''
                    SELECT
                    	TEXT_CODES.c_title_chn,
                    	TEXT_CODES.c_text_year,
                    	TEXT_ROLE_CODES.c_role_desc_chn,
                    	BIOG_TEXT_DATA.c_source,
                    	BIOG_TEXT_DATA.c_pages
                    FROM
                    	BIOG_TEXT_DATA
                    	LEFT JOIN TEXT_CODES ON BIOG_TEXT_DATA.c_textid = TEXT_CODES.c_textid
                    	LEFT JOIN TEXT_ROLE_CODES ON BIOG_TEXT_DATA.c_role_id = TEXT_ROLE_CODES.c_role_id
                    WHERE
                    	BIOG_TEXT_DATA.c_personid = \'{}\'
                           '''.format(pid))
            write_work_list_raw = cursor.fetchall()
            
            write_work_map = {}
            c_textids = [x[3] for x in write_work_list_raw]
            if c_textids:
                placeholders = ",".join(["?"] * len(c_textids))

                cursor.execute(
                    f'SELECT c_textid,c_title_chn FROM TEXT_CODES WHERE c_textid IN({placeholders})', c_textids)

                c_text_rows = cursor.fetchall()

                for text_id, title_name in c_text_rows:
                    if text_id not in write_work_map:
                        write_work_map[text_id] = title_name
            
            for write_work_set in write_work_list_raw:
                write_dic_each = {}
                write_work_key = write_work_set[2]
                if write_work_key in write_work_list:
                    write_dic_each['entity1'] = person_name
                    write_dic_each['entity1_info'] = ''
                    write_dic_each['entity1_category'] = '人名'
                    write_dic_each['entity2'] = str(write_work_set[0])
                    if write_work_set[1] is None:
                        write_start_year = '未详'
                    elif int(write_work_set[1]) == 0:
                        write_start_year = '未详'
                    else:
                        write_start_year = write_work_set[1]
                    if write_work_set[3] is not None:
                        source_id = write_work_set[3]
                    else:
                        source_id = '0'
                    text_src = write_work_map[source_id]
                    source = ''
                    if text_src is not None and len(text_src) != 0:
                        source = text_src
                    if write_work_set[4] is not None:
                        # if int(write_work_set[4]) != 0:
                        pagenum = True
                        try:
                            pagenum = int(write_work_set[4])
                            if pagenum == 0:
                                pagenum = False
                        except:
                            pass
                        source = source + \
                            '(頁{})'.format(write_work_set[4])
                    write_dic_each['entity2_info'] = '开始时间：{}；来源：{}'.format(
                        write_start_year, source)
                    # write_dic_each['entity2_category'] = write_work_key
                    write_dic_each['entity2_category'] = '作品'
                    if write_work_set[2] is not None:
                        write_dic_each['relationship'] = write_work_set[2]
                    else:
                        write_dic_each['relationship'] = '著'
                    res_list.append(write_dic_each)


    # =====================================================
    # 出生死亡时间
    # =====================================================

    times = json_data.get('times')

    if times is not None:
        if 'bd_time' in times:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT c_birthyear,c_deathyear FROM BIOG_MAIN WHERE c_personid = \'{}\''.format(pid))
            bd_time_raw_list = cursor.fetchall()
            for bd_time in bd_time_raw_list:
                bd_dic = {}
                birth_time = bd_time[0]
                death_time = bd_time[1]
                if birth_time is None or birth_time == 0:
                    birth_time = '未详'
                if death_time is None or death_time == 0:
                    death_time = '未详'
                bd_dic['entity1'] = person_name
                bd_dic['entity1_info'] = ''
                bd_dic['entity1_category'] = '人名'
                bd_dic['entity2'] = str(birth_time)
                bd_dic['entity2_info'] = ''
                bd_dic['entity2_category'] = '出生死亡日期'
                bd_dic['relationship'] = '出生时间'
                res_list.append(bd_dic)
                bd_dic = {}
                bd_dic['entity1'] = person_name
                bd_dic['entity1_info'] = ''
                bd_dic['entity1_category'] = '人名'
                bd_dic['entity2'] = str(death_time)
                bd_dic['entity2_info'] = ''
                bd_dic['entity2_category'] = '出生死亡日期'
                bd_dic['relationship'] = '死亡时间'
                res_list.append(bd_dic)
        # 关键时间 任职
        if 'work_time' in times:
            cursor = conn.cursor()
            cursor.execute('''
                        SELECT
                        	POSTED_TO_OFFICE_DATA.c_firstyear,
                        	POSTED_TO_OFFICE_DATA.c_lastyear,
                        	OFFICE_CODES.c_office_chn
                        FROM
                        	POSTED_TO_OFFICE_DATA 
                        LEFT JOIN OFFICE_CODES ON OFFICE_CODES.c_office_id = POSTED_TO_OFFICE_DATA.c_office_id
                        WHERE
                        	c_personid = \'{}\'
                           '''.format(pid))
            work_time_raw_list = cursor.fetchall()
            for work_time in work_time_raw_list:
                start_time = work_time[0]
                if start_time == 0:
                    start_time = '未详'
                end_time = work_time[1]
                if end_time == 0:
                    end_time = '未详'
                work_time_dic = {}
                work_time_dic['entity1'] = person_name
                work_time_dic['entity1_info'] = ''
                work_time_dic['entity1_category'] = '人名'
                work_time_dic['entity2'] = str(start_time)
                work_time_dic['entity2_info'] = '任期开始时间'
                work_time_dic['entity2_category'] = '任职时间'
                work_time_dic['relationship'] = '始任' + str(work_time[2])
                res_list.append(work_time_dic)
                work_time_dic = {}
                work_time_dic['entity1'] = person_name
                work_time_dic['entity1_info'] = ''
                work_time_dic['entity1_category'] = '人名'
                work_time_dic['entity2'] = str(end_time)
                work_time_dic['entity2_info'] = '卸任时间'
                work_time_dic['entity2_category'] = '任职时间'
                work_time_dic['relationship'] = '卸任于' + str(work_time[2])
                res_list.append(work_time_dic)
            
    # =====================================================
    # 游历地点
    # =====================================================

    places = json_data.get('places')
    
    if places is not None:
        places_dic = {
            "hometown": ["籍貫(基本地址)"],
            "born":     ["出生地"],
            "death":    ["葬地", '死所'],
            "travel": ["遊歷或曾經到過"],
            "exile":    ["流放之地"]
        }
        if len(places) != 0:
            places_list = []
            for place in places:
                for place_item in places_dic[place]:
                    places_list.append(place_item)
            cursor = conn.cursor()
            cursor.execute('''
                        SELECT
                        	ADDRESSES.c_name_chn,
                        	ADDRESSES.x_coord,
                        	ADDRESSES.y_coord,
                            BIOG_ADDR_CODES.c_addr_desc_chn,
                        	ADDRESSES.belongs1_Name,
                        	ADDRESSES.belongs2_Name,
                        	ADDRESSES.belongs3_Name,
                        	ADDRESSES.belongs4_Name,
                        	ADDRESSES.belongs5_Name
                        FROM
                        	BIOG_ADDR_DATA
                        	LEFT JOIN ADDRESSES ON BIOG_ADDR_DATA.c_addr_id = ADDRESSES.c_addr_id
                        	LEFT JOIN BIOG_ADDR_CODES ON BIOG_ADDR_DATA.c_addr_type = BIOG_ADDR_CODES.c_addr_type 
                        WHERE
                        	BIOG_ADDR_DATA.c_personid = \'{}\'
                           '''.format(pid))
            bd_addr_raw_list = cursor.fetchall()
            place_unique = set()
            for coord_set in bd_addr_raw_list:
                bd_addr_dic = {}
                coord_key = coord_set[3]
                if coord_key in places_list:
                    if str(coord_set[0])+str(coord_set[3]) in place_unique:
                        continue
                    place_unique.add(
                        str(coord_set[0])+str(coord_set[3]))
                    bd_addr_dic['entity1'] = person_name
                    bd_addr_dic['entity1_info'] = ''
                    bd_addr_dic['entity1_category'] = '人名'
                    bd_addr_dic['entity2'] = str(coord_set[0])
                    locate_list = []
                    for id in range(4, len(coord_set)):
                        if coord_set[id] is not None:
                            locate_list.append(coord_set[id])
                    locate_list.reverse()
                    coord_list_row = []
                    if coord_set[1] is not None:
                        coord_list_row.append(str(coord_set[1]))
                    else:
                        coord_list_row.append('0.0')
                    if coord_set[2] is not None:
                        coord_list_row.append(str(coord_set[2]))
                    else:
                        coord_list_row.append('0.0')
                    bd_addr_dic['entity2_info'] = '经纬度：{}；从属地区：{}'.format(
                        ','.join(coord_list_row), '，'.join(locate_list))
                    bd_addr_dic['entity2_category'] = coord_key
                    bd_addr_dic['relationship'] = coord_set[3]
                    res_list.append(bd_addr_dic)
    
    
    resp_dic['code'] = 200

    resp_dic['msg'] = '成功'

    resp_dic['data'] = res_list

    return json.dumps(
        resp_dic,
        ensure_ascii=False
    )

# =========================================================
# Handler
# =========================================================

class QueryNameMainHandler(tornado.web.RequestHandler):

    async def post(self):

        try:

            set_default_header(self)

            name_chn = self.get_argument(
                'nameChn',
                None
            )

            if not name_chn:

                self.write(
                    return_error(
                        1001,
                        '请求人名不能为空'
                    )
                )

                return

            logger.info(
                f'收到 queryName 请求: {name_chn}'
            )

            # 放线程池
            loop = asyncio.get_running_loop()

            result = await loop.run_in_executor(
                executor,
                query_name_logic,
                name_chn
            )

            self.write(result)

        except Exception as e:

            self.write(
                return_error(
                    500,
                    f'接口错误: {e}'
                )
            )


class QueryInfoHandler(tornado.web.RequestHandler):

    async def post(self):

        try:

            set_default_header(self)

            raw_data = self.request.body

            json_data = json.loads(
                raw_data
            )

            logger.info(
                '收到 queryInfo 请求'
            )

            loop = asyncio.get_running_loop()

            result = await loop.run_in_executor(
                executor,
                query_info_logic,
                json_data
            )

            self.write(result)

        except Exception as e:

            self.write(
                return_error(
                    500,
                    f'接口错误: {e}'
                )
            )


# =========================================================
# 主程序
# =========================================================

if __name__ == "__main__":

    port = 18612
    
    # 预热sqlite数据库
    warmup_sqlite_file()
    
    # 初始化数据库
    init_db()

    app = tornado.web.Application([
        (r"/queryName", QueryNameMainHandler),
        (r"/queryInfo", QueryInfoHandler),
    ])

    application = tornado.httpserver.HTTPServer(
        app,
        max_buffer_size=1048576000,
        max_body_size=1048576000
    )

    application.listen(port)

    print(f"====> 启动端口: {port}")

    logger.info(f"====> 启动端口: {port}")

    # 数据库保活
    PeriodicCallback(
        keep_db_hot,
        5 * 60 * 1000
    ).start()
    tornado.ioloop.IOLoop.current().start()
