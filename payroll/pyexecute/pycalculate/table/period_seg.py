# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class PeriodSeg:
    """
    Desc: 期间分段表 HHR_PY_RSLT_SEG
    Author: David
    Date: 2018/08/27
    """

    __slots__ = [
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'segment_num',
        'seg_bgn_dt',
        'seg_end_dt',
        'cal_days',
        'work_days',
        'legal_days',
        'cal_sum_days',
        'work_sum_days',
        'legal_sum_days'
    ]

    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.segment_num = kwargs.get('segment_num', 0)
        self.seg_bgn_dt = kwargs.get('seg_bgn_dt', None)
        self.seg_end_dt = kwargs.get('seg_end_dt', None)
        self.cal_days = kwargs.get('cal_days', 0)
        self.work_days = kwargs.get('work_days', 0)
        self.legal_days = kwargs.get('legal_days', 0)
        self.cal_sum_days = kwargs.get('cal_sum_days', 0)
        self.work_sum_days = kwargs.get('work_sum_days', 0)
        self.legal_sum_days = kwargs.get('legal_sum_days', 0)

    def insert(self):
        db = gv.get_db()
        run_parm = gv.get_run_var_value('RUN_PARM')
        seg = db.get_table('hhr_py_rslt_seg', schema_name='boogoo_payroll')
        ins = seg.insert()
        val = [
               {'tenant_id': self.tenant_id,
                'hhr_empid': self.emp_id,
                'hhr_emp_rcd': self.emp_rcd,
                'hhr_seq_num': self.seq_num,
                'hhr_segment_num': self.segment_num,
                'hhr_seg_bgn_dt': self.seg_bgn_dt,
                'hhr_seg_end_dt': self.seg_end_dt,
                'hhr_cal_days': self.cal_days,
                'hhr_work_days': self.work_days,
                'hhr_legal_days': self.legal_days,
                'hhr_cal_s_days': self.cal_sum_days,
                'hhr_work_s_days': self.work_sum_days,
                'hhr_legal_s_days': self.legal_sum_days,
                'hhr_create_dttm': get_current_dttm(),
                'hhr_create_user': run_parm['operator_user_id'],
                'hhr_modify_dttm': get_current_dttm(),
                'hhr_modify_user': run_parm['operator_user_id'],
                }]
        db.conn.execute(ins, val)
