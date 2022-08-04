#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ...celeryapp import app
from ...dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
# import traceback


@app.task(name='pyidentify', base=DatabaseTask, bind=True, serializer='json')
def pyidentify(self, **kwargs):
    """
    Desc: 标记受款人
    Author: David
    Date: 2018/08/09
    """

    from ...payroll.pyexecute.pymain import run_dist_py_engine

    # log_file = self.create_file(self, 'pyidentify.log')
    # log_file.write_line('开始标记受款人......')

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
        pass
        # log_file.write_line(traceback.format_exc())
        raise
    finally:
        # log_file.close()
        self.conn.close()
        pass
    return

