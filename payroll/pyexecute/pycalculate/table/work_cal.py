# -*- coding: utf-8 -*-

from ....pysysutils import global_variables as gv
from .....utils import get_current_dttm


class WorkCal:
    """
    Desc: 薪资结果-工作日历表 HHR_PY_RSLT_WKCAL
    Author: David
    Date: 2018/08/28
    """

    __slots__ = [
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'seq_num',
        'shift_dt',
        'shift_id',
        'active',
        'is_legal',
        'work_hours',
        'days_convert',
        'shift_type',
        'holiday_type',
        'wrk_pln_id'
    ]

    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 1)
        self.seq_num = kwargs.get('seq_num', 0)
        self.shift_dt = kwargs.get('shift_dt', None)
        self.shift_id = kwargs.get('shift_id', '')
        self.active = kwargs.get('active', '')
        self.is_legal = kwargs.get('is_legal', '')
        self.work_hours = kwargs.get('work_hours', 0)
        self.days_convert = kwargs.get('days_convert', 0)
        self.shift_type = kwargs.get('shift_type', '')
        self.holiday_type = kwargs.get('holiday_type', '')
        self.wrk_pln_id  = kwargs.get('wrk_pln_id', '')

    def insert(self):
        run_parm = gv.get_run_var_value('RUN_PARM')
        db = gv.get_db()
        seg = db.get_table('hhr_py_rslt_wkcal', schema_name='boogoo_payroll')
        ins = seg.insert()
        val = [
               {'tenant_id': self.tenant_id,
                'hhr_empid': self.emp_id,
                'hhr_emp_rcd': self.emp_rcd,
                'hhr_seq_num': self.seq_num,
                'hhr_date': self.shift_dt,
                'hhr_shift_id': self.shift_id,
                'hhr_active': self.active,
                'hhr_is_legal': self.is_legal,
                'hhr_work_hours': self.work_hours,
                'hhr_days_conver': self.days_convert,
                'hhr_shift_type': self.shift_type,
                'hhr_holiday_type': self.holiday_type,
                'hhr_work_plan_code': self.wrk_pln_id,
                'hhr_create_dttm': get_current_dttm(),
                'hhr_create_user': run_parm['operator_user_id'],
                'hhr_modify_dttm': get_current_dttm(),
                'hhr_modify_user': run_parm['operator_user_id'],
                }]
        db.conn.execute(ins, val)
