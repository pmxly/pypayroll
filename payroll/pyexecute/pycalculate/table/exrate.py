# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from sqlalchemy.sql import text


class ExchangeRate:
    """
    Desc: 汇率
    Author: David
    Date: 2018/09/06
    """

    __slots__ = ['tenant_id', 'from_cur', 'to_cur', 'from_amt', 'to_amt']

    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.from_cur = kwargs.get('from_cur', '')
        self.to_cur = kwargs.get('to_cur', '')
        self.from_amt = kwargs.get('from_amt', 0)
        self.to_amt = kwargs.get('to_amt', 0)


def init_ex_rate(**kwargs):
    """
    Desc: 根据历经期结束日期初始化汇率
    Author: David
    Date: 2018/09/06
    """

    catalog = gv.get_run_var_value('PY_CATALOG')
    tenant_id = catalog.tenant_id
    f_end_date = catalog.f_prd_end_dt

    ex_rate_dic = gv.get_run_var_value('EX_RATE_DIC')
    if ex_rate_dic is None:
        ex_rate_dic = dict()
        ex_rate_dic[f_end_date] = []
    else:
        if f_end_date not in ex_rate_dic:
            ex_rate_dic[f_end_date] = []
    if len(ex_rate_dic[f_end_date]) == 0:
        db = gv.get_db()

        s = text("select a.hhr_from_cur, a.hhr_to_cur, a.hhr_from_amt, a.hhr_to_amt from boogoo_payroll.hhr_py_exrate a "
                 "where a.tenant_id = :b1 and a.hhr_status = 'Y' "
                 "and :b2 between a.hhr_efft_date and a.hhr_efft_end_date "
                 # "and a.hhr_efft_date = (select max(a1.hhr_efft_date) from boogoo_payroll.hhr_py_exrate a1 "
                 # " where a1.tenant_id = a.tenant_id and a1.hhr_from_cur = a.hhr_from_cur "
                 # " and a1.hhr_to_cur = a.hhr_to_cur and a1.hhr_efft_date <= :b2) "
                 "")
        rs = db.conn.execute(s, b1=tenant_id, b2=f_end_date).fetchall()
        for row in rs:
            from_cur = row['hhr_from_cur']
            to_cur = row['hhr_to_cur']
            from_amt = row['hhr_from_amt']
            to_amt = row['hhr_to_amt']
            row_dict = {'tenant_id': tenant_id, 'from_cur': from_cur, 'to_cur': to_cur, 'from_amt': from_amt,
                        'to_amt': to_amt}
            cur_ex = ExchangeRate(**row_dict)
            ex_rate_dic[f_end_date].append(cur_ex)
        gv.set_run_var_value('EX_RATE_DIC', ex_rate_dic)
