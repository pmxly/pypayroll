# -*- coding: utf-8 -*-

from ....celeryapp import app
from ....dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
import traceback
from ....utils import get_current_dttm


@app.task(name='om_data_sync', base=DatabaseTask, bind=True)
def om_data_sync(self, **kwargs):
    """
    Desc: 组织数据同步
    Author: David
    Date: 2019/03/18
    """

    from ....soa.dahao.data_sync_utils import invoke_dept_sync_srv, invoke_posn_sync_srv

    sys_code = kwargs.get('sys_code', None)
    try:
        trans = self.conn.begin()
        try:
            ifc_call_dtm = get_current_dttm()

            invoke_dept_sync_srv(self.conn, sys_code, ifc_call_dtm)
            invoke_posn_sync_srv(self.conn, sys_code, ifc_call_dtm)
            trans.commit()
        except Exception:
            trans.rollback()
            raise

    except SoftTimeLimitExceeded:
        print('用户撤销了进程或者超过了Soft time限制')
    except Exception:
        log_file = self.create_file(self, 'om_data_sync.log')
        log_file.write_line(traceback.format_exc())
        log_file.close()
        raise
    finally:
        self.conn.close()
    return 1
