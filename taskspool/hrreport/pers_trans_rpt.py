#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from ...dbtask import DatabaseTask
from ...celeryapp import app


@app.task(name='pers_trans_rpt', base=DatabaseTask, bind=True)
def pers_trans_rpt(self, **kwargs):
    """
    Desc: 生成人事异动情况表
    Author: 陶雨
    Date: 2019/04/22
    """
    
    from ...report.hr_rpt_main import run_pers_trans_rpt

    try:
        run_param = {}
        run_param['user_id'] = kwargs.get('user_id', None)
        run_param['tenant_id'] = kwargs.get('tenant_id', None)
        run_param['lang'] = kwargs.get('lang', None)
        run_param['start_date'] = kwargs.get('start_date', None)
        run_param['end_date'] = kwargs.get('end_date', None)
        run_param['dept_cd'] = kwargs.get('dept_cd', None)
        run_param['has_child_dept'] = kwargs.get('has_child_dept', None)
        run_param['company'] = kwargs.get('company', None)
        run_param['hr_sub_range'] = kwargs.get('hr_sub_range', None)
        run_param['emp_class'] = kwargs.get('emp_class', None)
        run_param['emp_sub_class'] = kwargs.get('emp_sub_class', None)
        if run_param['lang'] == 'zh_CN':
            run_param['excel_file_path'] = self.get_excel_file_path(self, '人事异动情况表.xlsx')
        else:
            run_param['excel_file_path'] = self.get_excel_file_path(self, 'PersonnelChangeSchedule.xlsx')
        run_pers_trans_rpt(run_param, self.conn)

    except Exception:
        pass
        raise
    finally:
        self.conn.close()
    return
