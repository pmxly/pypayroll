# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class PinSubCls:
    """
    Desc: 薪资结果-薪资项目子类 hhr_py_rslt_pin_sub_cls
    Author: David
    Date: 2020/12/10
    """

    __slots__ = [
        'db',
        'run_parm',
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'pin_cd',
        'pin_sub_cls_cd',
        'count',
        'unit',
        'pin_sub_cls_type',
        'comment'
    ]

    def __init__(self, **kwargs):
        self.db = gv.get_db()
        self.run_parm = gv.get_run_var_value('RUN_PARM')
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.pin_cd = kwargs.get('pin_cd', '')
        self.pin_sub_cls_cd = kwargs.get('pin_sub_cls_cd', '')
        self.count = kwargs.get('count', 0)
        self.unit = kwargs.get('unit', '')
        self.pin_sub_cls_type = kwargs.get('pin_sub_cls_type', '')
        self.comment = kwargs.get('comment', '')

    def insert(self):
        pin_sub = self.db.get_table('hhr_py_rslt_pin_sub_cls', schema_name='boogoo_payroll')
        ins = pin_sub.insert()
        val = [
            {'tenant_id': self.tenant_id,
             'hhr_empid': self.emp_id,
             'hhr_emp_rcd': self.emp_rcd,
             'hhr_seq_num': self.seq_num,
             'hhr_pin_cd': self.pin_cd,
             'hhr_pin_sub_cls_cd': self.pin_sub_cls_cd,
             'hhr_count': self.count,
             'hhr_unit': self.unit,
             'hhr_pin_sub_cls_type': self.pin_sub_cls_type,
             'hhr_comment': self.comment,
             'hhr_create_dttm': get_current_dttm(),
             'hhr_create_user': self.run_parm['operator_user_id'],
             'hhr_modify_dttm': get_current_dttm(),
             'hhr_modify_user': self.run_parm['operator_user_id'],
             }]
        self.db.conn.execute(ins, val)
