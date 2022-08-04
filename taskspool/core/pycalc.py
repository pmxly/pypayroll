#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ...celeryapp import app
from ...dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
# import traceback
import sys


@app.task(name='pycalc', base=DatabaseTask, bind=True, serializer='json')
def pycalc(self, **kwargs):
    """
    Desc: 薪资计算进程
    Author: David
    Date: 2018/08/09
    """

    # 清除已经加载过的相关模块，解决打开和关闭日志的bug
    key_lst = list(sys.modules.keys())
    for k in key_lst:
        if 'hhr.payroll' in k:
            sys.modules.pop(k)

    from ...payroll.pyexecute.pymain import run_dist_py_engine

    # log_file = self.create_file(self, 'pycalc.log')
    # log_file.write_line('开始薪资计算......')

    try:
        task_id = self.request.id

        run_param = kwargs.get('run_param', None)
        run_param['task_id'] = task_id
        run_param['task_db'] = self

        run_dist_py_engine(run_param)

        # log_file.write_line('处理完成......')
    except SoftTimeLimitExceeded:
        # log_file.write_line('Task revoked by user request or Soft time limit exceeded')
        print('Task revoked by user request or Soft time limit exceeded')
    except Exception:
        # log_file.write_line(traceback.format_exc())
        raise
    finally:
        self.conn.close()
        # log_file.close()
    return
