from datetime import datetime
from .py_rpt_data import (get_default_tmpl_cd, get_rept_tmpl_lines, run_py_dtl_by_cal, run_py_dtl_by_date, run_py_sum)


def run_py_rpt_detail(run_param, conn):
    """
    Desc: 生成薪资明细表
    Author: David
    Date: 2019/03/04
    """

    start_date = run_param['start_date']
    end_date = run_param['end_date']

    if start_date and isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        run_param['start_date'] = start_date
    if end_date and isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        run_param['end_date'] = end_date

    run_param['conn'] = conn
    tenant_id = run_param['tenant_id']
    lang = run_param['lang']
    py_cal_id = run_param['py_cal_id']
    col_trac_detail = run_param['col_trac_detail']
    tmpl_cd = run_param['tmpl_cd']
    if not tmpl_cd:
        tmpl_cd = get_default_tmpl_cd(conn, tenant_id, 'A')
    tmpl_line_list = get_rept_tmpl_lines(conn, tenant_id, lang, tmpl_cd)

    by_cal = by_trac_detail = False
    if py_cal_id:
        by_cal = True

    if col_trac_detail == 'Y':
        by_trac_detail = True

    # 传递了日历参数
    if by_cal:
        run_py_dtl_by_cal(run_param, tmpl_line_list, by_trac_detail)

    # 不传日历参数
    if not by_cal:
        run_py_dtl_by_date(run_param, tmpl_line_list, by_trac_detail)


def run_py_rpt_sum(run_param, conn):
    """
    Desc: 生成薪资汇总表
    Author: David
    Date: 2019/03/04
    """

    start_date = run_param['start_date']
    end_date = run_param['end_date']

    if start_date and isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        run_param['start_date'] = start_date
    if end_date and isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        run_param['end_date'] = end_date

    run_param['conn'] = conn
    tenant_id = run_param['tenant_id']
    lang = run_param['lang']
    py_cal_id = run_param['py_cal_id']
    tmpl_cd = run_param.get('tmpl_cd', '')
    if not tmpl_cd:
        tmpl_cd = get_default_tmpl_cd(conn, tenant_id, 'B')
    tmpl_line_list = get_rept_tmpl_lines(conn, tenant_id, lang, tmpl_cd)

    by_cal = False
    if py_cal_id:
        by_cal = True

    # 按日历统计
    if by_cal:
        run_py_sum(run_param, tmpl_line_list, False, 'C')

    # 按日期统计
    if not by_cal:
        run_py_sum(run_param, tmpl_line_list, False, 'D')
