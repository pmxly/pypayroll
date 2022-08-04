# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class SegFactor:
    """
    Desc: 分段因子 HHR_PY_RSLT_SEGFACT
    Author: David
    Date: 2018/08/28
    """

    __slots__ = [
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'seg_rule_cd',
        'segment_num',
        'numerator_val',
        'denominator_val',
        'factor_val'
    ]

    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.seg_rule_cd = kwargs.get('seg_rule_cd', '')
        self.segment_num = kwargs.get('segment_num', 0)
        self.numerator_val = kwargs.get('numerator_val', 0)
        self.denominator_val = kwargs.get('denominator_val', 0)
        self.factor_val = kwargs.get('factor_val', 0)

    def insert(self):
        db = gv.get_db()
        run_parm = gv.get_run_var_value('RUN_PARM')
        seg_fact = db.get_table('hhr_py_rslt_segfact', schema_name='boogoo_payroll')
        ins = seg_fact.insert()
        val = [
               {'tenant_id': self.tenant_id,
                'hhr_empid': self.emp_id,
                'hhr_emp_rcd': self.emp_rcd,
                'hhr_seq_num': self.seq_num,
                'hhr_seg_rule_cd': self.seg_rule_cd,
                'hhr_segment_num': self.segment_num,
                'hhr_numer_val': self.numerator_val,
                'hhr_denom_val': self.denominator_val,
                'hhr_factor_val': self.factor_val,
                'hhr_create_dttm': get_current_dttm(),
                'hhr_create_user': run_parm['operator_user_id'],
                'hhr_modify_dttm': get_current_dttm(),
                'hhr_modify_user': run_parm['operator_user_id'],
                }]
        db.conn.execute(ins, val)
