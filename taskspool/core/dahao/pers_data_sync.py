# -*- coding: utf-8 -*-

from ....celeryapp import app
from ....dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
import traceback
from ....utils import get_current_dttm


@app.task(name='pers_data_sync', base=DatabaseTask, bind=True)
def pers_data_sync(self, **kwargs):
    """
    Desc: 个人信息同步
    Author: David
    Date: 2019/03/18
    """

    from ....soa.dahao.data_sync_utils import invoke_job_data_sync_srv, invoke_pers_sync_srv

    sys_code = kwargs.get('sys_code', None)
    try:
        trans = self.conn.begin()
        try:
            ifc_call_dtm = get_current_dttm()
            # del_lst存放元组（emp_id, itc_type），del_lst中的每条记录表示某个员工某个信息类型的所有数据已经被删除
            del_lst = []
            invoke_job_data_sync_srv(self.conn, sys_code, ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'TITL', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'BIO', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'IDEN', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'ADDR', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'ARCH', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'CERT', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'PROB', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'CONT', ifc_call_dtm, del_lst)
            # invoke_pers_sync_srv(self.conn, sys_code, 'PROT', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'CRED', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'PERF', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'TRAN', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'PHON', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'MAIL', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'EDU', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'RWRD', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'RELA', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'WORK', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'LANG', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'BANK', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'DEL', ifc_call_dtm, del_lst)
            invoke_pers_sync_srv(self.conn, sys_code, 'IFDL', ifc_call_dtm, del_lst)
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
