#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from ...dbtask import DatabaseTask
from ...celeryapp import app


@app.task(name='py_report_sum', base=DatabaseTask, bind=True)
def py_report_sum(self, **kwargs):
    """
    Desc: 生成薪资汇总表
    Author: 陶雨
    Date: 2019/01/16
    """
    
    from ...report.py_rpt_main import run_py_rpt_sum

    try:
        run_param = {}
        run_param['user_id'] = kwargs.get('user_id', None)
        run_param['tenant_id'] = kwargs.get('tenant_id', None)
        run_param['lang'] = kwargs.get('lang', None)
        run_param['tmpl_cd'] = kwargs.get('tmpl_cd', None)
        run_param['py_cal_id'] = kwargs.get('py_cal_id', None)
        run_param['py_group_id'] = kwargs.get('py_group_id', None)
        run_param['start_date'] = kwargs.get('start_date', None)
        run_param['end_date'] = kwargs.get('end_date', None)
        run_param['dept_cd'] = kwargs.get('dept_cd', None)
        run_param['has_child_dept'] = kwargs.get('has_child_dept', None)
        run_param['tree_lvl'] = kwargs.get('tree_lvl', None)
        if run_param['lang'] == 'zh_CN':
            run_param['excel_file_path'] = self.get_excel_file_path(self, '薪资汇总表.xlsx')
        else:
            run_param['excel_file_path'] = self.get_excel_file_path(self, 'PayrollSummary.xlsx')
        run_py_rpt_sum(run_param, self.conn)

    except Exception:
        pass
        raise
    finally:
        self.conn.close()
    return
