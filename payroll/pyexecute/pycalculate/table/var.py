# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class Var:
    """
    Desc: 薪资结果-变量表 HHR_PY_RSLT_VAR
    Author: David
    Date: 2018/09/06
    """

    __slots__ = [
        'db',
        'run_parm',
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'var_id',
        'var_type',
        'var_char',
        'var_dt',
        'var_dec',
        'prc_flag'
    ]

    def __init__(self, action='I', **kwargs):
        self.db = gv.get_db()
        self.run_parm = gv.get_run_var_value('RUN_PARM')
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.var_id = kwargs.get('var_id', '')

        if action == 'I':
            self.var_type = kwargs.get('var_type', '')
            self.var_char = kwargs.get('var_val_char', '')
            self.var_dt = kwargs.get('var_dt', None)
            self.var_dec = kwargs.get('var_val_dec', 0)
            self.prc_flag = kwargs.get('prc_flag', '')

    def insert(self):
        var = self.db.get_table('hhr_py_rslt_var', schema_name='boogoo_payroll')
        ins = var.insert()
        val = [
            {'tenant_id': self.tenant_id,
             'hhr_empid': self.emp_id,
             'hhr_emp_rcd': self.emp_rcd,
             'hhr_seq_num': self.seq_num,
             'hhr_variable_id': self.var_id,
             'hhr_data_type': self.var_type,
             'hhr_varval_char': self.var_char,
             'hhr_varval_date': self.var_dt,
             'hhr_varval_dec': self.var_dec,
             'hhr_prcs_flag': self.prc_flag,
             'hhr_create_dttm': get_current_dttm(),
             'hhr_create_user': self.run_parm['operator_user_id'],
             'hhr_modify_dttm': get_current_dttm(),
             'hhr_modify_user': self.run_parm['operator_user_id'],
             }]
        self.db.conn.execute(ins, val)

    def update(self, **values):
        var = self.db.get_table('hhr_py_rslt_var', schema_name='boogoo_payroll')
        values['hhr_modify_dttm'] = get_current_dttm()
        values['hhr_modify_user'] = self.run_parm['operator_user_id']
        u = var.update(). \
            where(var.c.tenant_id == self.tenant_id).\
            where(var.c.hhr_empid == self.emp_id).\
            where(var.c.hhr_emp_rcd == self.emp_rcd).\
            where(var.c.hhr_seq_num == self.seq_num).\
            where(var.c.hhr_variable_id == self.var_id).\
            values(values)
        self.db.conn.execute(u)
