# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class PinAccTable:
    """
    Desc: 薪资结果-薪资项目累计 HHR_PY_RSLT_ACCM
    Author: David
    Date: 2018/10/25
    """

    __slots__ = [
        'db',
        'run_parm',
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'acc_cd',
        'acc_type',
        'add_year',
        'add_num',
        'amt',
        'currency',
        'quantity',
        'quantity_unit'
    ]

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.run_parm = gv.get_run_var_value('RUN_PARM')
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.acc_cd = kwargs.get('acc_cd', '')
        self.acc_type = kwargs.get('acc_type', '')
        self.add_year = kwargs.get('add_year', 1900)
        self.add_num = kwargs.get('add_num', 0)
        self.amt = kwargs.get('amt', 0.0)
        self.currency = kwargs.get('currency', '')
        self.quantity = kwargs.get('quantity', 0)
        self.quantity_unit = kwargs.get('quantity_unit', '')

        if not self.currency:
            self.currency = gv.get_var_value('VR_PG_CURRENCY')

    def insert(self):
        pin_seg = self.db.get_table('hhr_py_rslt_accm', schema_name='boogoo_payroll')
        if self.seq_num == "*":
            self.seq_num = 999
        ins = pin_seg.insert()
        val = [
            {'tenant_id': self.tenant_id,
             'hhr_empid': self.emp_id,
             'hhr_emp_rcd': self.emp_rcd,
             'hhr_seq_num': self.seq_num,
             'hhr_acc_cd': self.acc_cd,
             'hhr_accm_type': self.acc_type,
             'hhr_period_add_year': self.add_year,
             'hhr_period_add_number': self.add_num,
             'hhr_amt': self.amt,
             'hhr_currency': self.currency,
             'hhr_quantity': self.quantity,
             'hhr_quanty_unit': self.quantity_unit,
             'hhr_create_dttm': get_current_dttm(),
             'hhr_create_user': self.run_parm['operator_user_id'],
             'hhr_modify_dttm': get_current_dttm(),
             'hhr_modify_user': self.run_parm['operator_user_id'],
             }]
        self.db.conn.execute(ins, val)
