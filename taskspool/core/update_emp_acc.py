#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ...celeryapp import app
from ...dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
from ...hr.bus_utils import upd_emp_account
from ...utils import get_current_date


@app.task(name='update_emp_acc', base=DatabaseTask, bind=True, serializer='json')
def update_emp_acc(self, **kwargs):
    """
    Desc: 新建/注销员工账号
    Author: David
    Date: 2019/03/07
    """

    try:
        tenant_id = kwargs.get('tenant_id', 0)
        emp_id = kwargs.get('emp_id', '')
        eff_dt = get_current_date()
        upd_emp_account(self.conn, tenant_id, emp_id, eff_dt)
    except SoftTimeLimitExceeded:
        print('Task revoked by user request or Soft time limit exceeded')
    except Exception:
        raise
    finally:
        self.conn.close()
    return
