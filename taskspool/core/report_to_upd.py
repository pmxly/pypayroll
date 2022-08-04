#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ...celeryapp import app
from ...dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
from ...utils import get_current_dttm
from ...hr.bus_utils import refresh_rpt_data


@app.task(name='report_to_upd', base=DatabaseTask, bind=True, serializer='json')
def report_to_upd(self, **kwargs):
    """
    Desc: 刷新职位、人员汇报关系
    Author: David
    Date: 2019/01/17
    """

    # log_file = self.create_file(self, 'pycalc.log')
    # log_file.write_line('开始刷新......')

    try:
        tenant_id = kwargs.get('tenant_id', 0)
        eff_dt = kwargs.get('eff_dt', get_current_dttm())
        if eff_dt is None:
            eff_dt = get_current_dttm()
        refresh_rpt_data(self.conn, tenant_id, eff_dt)
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
