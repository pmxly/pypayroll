# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm
from sqlalchemy import select
from sqlalchemy.sql import text


class AccumulateCalendar:

    __slots__ = [
        'period_cd',
        'period_year',
        'period_num',
        'add_type',
        'add_year',
        'add_num'
    ]

    def __init__(self, **kwargs):
        self.period_cd = kwargs.get('period_cd', '')
        self.period_year = kwargs.get('period_year', 0)
        self.period_num = kwargs.get('period_num', 0)
        self.add_type = kwargs.get('add_type', '')
        self.add_year = kwargs.get('add_year', '')
        self.add_num = kwargs.get('add_num', '')


def init_acc_cal(**kwargs):
    """
    Desc: 初始化累计日历
    Author: David
    Date: 2018/09/06
    """

    catalog = kwargs.get('catalog', None)

    tenant_id = catalog.tenant_id
    emp_id = catalog.emp_id
    emp_rcd = catalog.emp_rcd
    seq_num = catalog.seq_num

    period_cd = catalog.f_period_cd
    period_year = catalog.f_period_year
    period_num = catalog.f_prd_num

    acc_key = str(period_cd) + str(period_year) + str(period_num)
    acc_cal_dic = gv.get_run_var_value('ACC_CAL_DIC')
    if acc_cal_dic is None:
        acc_cal_dic = dict()
        acc_cal_dic[acc_key] = []
    else:
        if acc_key not in acc_cal_dic:
            acc_cal_dic[acc_key] = []
    if len(acc_cal_dic[acc_key]) == 0:
        db = gv.get_db()
        stmt = text("select hhr_period_add_type, hhr_period_add_year, hhr_period_add_number from boogoo_payroll.hhr_py_period_add_calendar_line "
                    "where tenant_id = :b1 and hhr_period_code = :b2 and hhr_period_year = :b3 and hhr_prd_num = :b4 ")
        rs = db.conn.execute(stmt, b1=tenant_id, b2=period_cd, b3=period_year, b4=period_num).fetchall()
        for row in rs:
            add_type = row['hhr_period_add_type']
            add_year = row['hhr_period_add_year']
            add_num = row['hhr_period_add_number']
            row_dict = {'period_cd': period_cd, 'period_year': period_year, 'period_num': period_num, 'add_type': add_type,
                        'add_year': add_year, 'add_num': add_num}
            acc_cal = AccumulateCalendar(**row_dict)
            acc_cal_dic[acc_key].append(acc_cal)
        gv.set_run_var_value('ACC_CAL_DIC', acc_cal_dic)
