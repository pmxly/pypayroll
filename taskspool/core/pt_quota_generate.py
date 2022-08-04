#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ...celeryapp import app
from ...dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
from ...pt.pt_utils import generate_emp_leave_quota


@app.task(name='pt_quota_generate', base=DatabaseTask, bind=True, serializer='json')
def pt_quota_generate(self, **kwargs):
    """
    Desc: 自动生成假期额度
    Author: David
    Date: 2019/01/22
    """

    # log_file = self.create_file(self, 'pycalc.log')
    # log_file.write_line('开始生成......')

    try:
        tenant_id = kwargs.get('tenant_id', None)
        generate_emp_leave_quota(self.conn, tenant_id)

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
