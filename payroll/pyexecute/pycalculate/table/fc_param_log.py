# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class ParamLog:
    """
    Desc: 薪资计算函数参数过程日志 HHR_PY_FC_PARAM_LOG
    Author: David
    Date: 2018/11/29
    """

    __slots__ = [
        'db',
        'run_parm',
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'tree_node_num',
        'cal_id',
        'cur_cal_id',
        'in_out_flag',
        'param_id',
        'param_val'
    ]

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.run_parm = gv.get_run_var_value('RUN_PARM')
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.tree_node_num = kwargs.get('tree_node_num', 0)
        self.cal_id = kwargs.get('cal_id', '')
        self.cur_cal_id = kwargs.get('cur_cal_id', '')
        self.in_out_flag = kwargs.get('in_out_flag', '')
        self.param_id = kwargs.get('param_id', '')
        self.param_val = kwargs.get('param_val', '')

    def insert(self):
        param_log = self.db.get_table('hhr_py_fc_param_log', schema_name='boogoo_payroll')
        ins = param_log.insert()
        val = [
            {'tenant_id': self.tenant_id,
             'hhr_empid': self.emp_id,
             'hhr_emp_rcd': self.emp_rcd,
             'hhr_seq_num': self.seq_num,
             'hhr_tree_node_num': self.tree_node_num,
             'hhr_py_cal_id': self.cal_id,
             'hhr_f_cal_id': self.cur_cal_id,
             'hhr_bf_af_flag': self.in_out_flag,
             'hhr_param_id': self.param_id,
             'hhr_param_val': self.param_val,
             'hhr_create_dttm': get_current_dttm(),
             'hhr_create_user': self.run_parm['operator_user_id'],
             'hhr_modify_dttm': get_current_dttm(),
             'hhr_modify_user': self.run_parm['operator_user_id'],
             }]
        self.db.conn.execute(ins, val)
