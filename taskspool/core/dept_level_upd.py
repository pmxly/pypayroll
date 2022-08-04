#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ...celeryapp import app
from ...dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
from ...utils import get_current_date
from ...hr.bus_utils import upd_dept_rpt_data, get_dept_lvl_upd_date_limit, get_dept_lvl_upd_date_str
from datetime import datetime
from sqlalchemy.sql import text
from ...utils import redis_master


@app.task(name='dept_level_upd', base=DatabaseTask, bind=True, serializer='json')
def dept_level_upd(self, **kwargs):
    """
    Desc: 刷新部门层级关系
    Author: David
    Date: 2019/01/17
    """

    # log_file = self.create_file(self, 'dept_level_upd.log')
    # log_file.write_line('开始刷新......')

    conn = self.conn
    trans = conn.begin()
    conn.execute("SET autocommit= 0 ")
    try:
        tenant_id = kwargs.get('tenant_id', 0)
        # effdt_string = kwargs.get('effdt_string', '')
        effdt_string = redis_master.get("dept_level_upd_effdt_string").decode("utf-8")
        sync_all_flag = kwargs.get('sync_all_flag', 'N')

        # 获取系统支持的语言代码
        lang_template_dic = {}
        lang_sel_stmt = text("select code from hzero_platform.fd_language")
        lang_rs = conn.execute(lang_sel_stmt).fetchall()

        for lang_row in lang_rs:
            lang_cd = lang_row['code']
            lang_template_dic[lang_cd] = []

        date_limit = int(get_dept_lvl_upd_date_limit(conn, tenant_id))

        if effdt_string == '':
            date_list_str = get_dept_lvl_upd_date_str(conn, tenant_id)
            if date_list_str != '' and date_list_str != "''":
                effdt_str_list = date_list_str.split(',')
            else:
                return 1
        else:
            effdt_str_list = effdt_string.split(',')

        i = 0
        for effdt_str in effdt_str_list:
            i = i + 1
            if i > date_limit:
                break
            if effdt_str.strip():
                eff_dt = datetime.strptime(effdt_str, "%Y-%m-%d")
                upd_dept_rpt_data(conn, tenant_id, eff_dt, lang_template_dic)
        # 刷新当前层级数据
        if i > 0:
            upd_dept_rpt_data(conn, tenant_id, get_current_date(), lang_template_dic, isCur=True)
        trans.commit()

    except SoftTimeLimitExceeded:
        trans.rollback()
        # log_file.write_line('Task revoked by user request or Soft time limit exceeded')
        print('Task revoked by user request or Soft time limit exceeded')
    except Exception:
        trans.rollback()
        # log_file.write_line(traceback.format_exc())
        raise
    finally:
        conn.close()
        # log_file.close()
    return 1
