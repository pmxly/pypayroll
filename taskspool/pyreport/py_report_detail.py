#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from ...dbtask import DatabaseTask
from ...celeryapp import app
from celery.utils.log import get_task_logger


logger = get_task_logger(__name__)


@app.task(name='py_report_detail', base=DatabaseTask, bind=True)
def py_report_detail(self, **kwargs):
    """
    Desc: 生成薪资明细表
    Author: 陶雨
    Date: 2019/01/16
    """

    from ...report.py_rpt_main import run_py_rpt_detail

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
        run_param['emp_id'] = kwargs.get('emp_id', None)
        run_param['emp_rcd'] = kwargs.get('emp_rcd', None)
        run_param['col_trac_detail'] = kwargs.get('col_trac_detail', None)
        if run_param['lang'] == 'zh_CN':
            run_param['excel_file_path'] = self.get_excel_file_path(self, '薪资明细表.xlsx')
        else:
            run_param['excel_file_path'] = self.get_excel_file_path(self, 'PayrollDetail.xlsx')

        run_py_rpt_detail(run_param, self.conn)

    except Exception:
        pass
        raise
    finally:
        self.conn.close()
    return




