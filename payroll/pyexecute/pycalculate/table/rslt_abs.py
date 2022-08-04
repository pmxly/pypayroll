# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class Abs:
    """
    Desc: 薪资计算考勤结果
    Author: David
    Date: 2019/10/22
    """

    __slots__ = [
        'db',
        'run_parm',
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'abs_cd',
        'abs_seq_num',
        'abs_date',
        'quantity',
        'unit',
        'date_type',
        'year',
        'month',
        'belong_year',
        'belong_month'
    ]

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.run_parm = gv.get_run_var_value('RUN_PARM')
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 0)
        self.seq_num = kwargs.get('seq_num', 0)
        self.abs_cd = kwargs.get('abs_cd', '')
        self.abs_seq_num = kwargs.get('abs_seq_num', 0)
        self.abs_date = kwargs.get('abs_date', None)
        self.quantity = kwargs.get('quantity', 0)
        self.unit = kwargs.get('unit', '')
        self.date_type = kwargs.get('date_type', '')
        self.year = kwargs.get('year', 1900)
        self.month = kwargs.get('month', 0)
        self.belong_year = kwargs.get('belong_year', 1900)
        self.belong_month = kwargs.get('belong_month', 0)

    def insert(self):
        abs_t = self.db.get_table('hhr_py_rslt_abs', schema_name='boogoo_payroll')
        ins = abs_t.insert()
        val = [
            {'tenant_id': self.tenant_id,
             'hhr_empid': self.emp_id,
             'hhr_emp_rcd': self.emp_rcd,
             'hhr_seq_num': self.seq_num,
             'hhr_abs_code': self.abs_cd,
             'hhr_seqnum': self.abs_seq_num,
             'hhr_date': self.abs_date,
             'hhr_quantity': self.quantity,
             'hhr_unit': self.unit,
             'hhr_holiday_type': self.date_type,
             'hhr_year': self.year,
             'hhr_month': self.month,
             'hhr_belong_year': self.belong_year,
             'hhr_belong_month': self.belong_month,
             'hhr_create_dttm': get_current_dttm(),
             'hhr_create_user': self.run_parm['operator_user_id'],
             'hhr_modify_dttm': get_current_dttm(),
             'hhr_modify_user': self.run_parm['operator_user_id'],
             }]
        self.db.conn.execute(ins, val)
