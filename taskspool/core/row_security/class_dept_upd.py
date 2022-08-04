# -*- coding: utf-8 -*-

from ....celeryapp import app
from celery.utils.log import get_task_logger
from ....dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
import traceback
from sqlalchemy import select, text
from ....utils import get_current_dttm
from ....hr.bus_utils import get_child_depts, get_cfg_sw_val
from sqlalchemy.exc import IntegrityError

logger = get_task_logger(__name__)


@app.task(name='class_dept_upd', base=DatabaseTask, bind=True)
def class_dept_upd(self, **kwargs):
    """
    Desc: 刷新部门安全性
    Author: David
    Date: 2019/03/11

    # Field: hhr_subdept_option:
    # C - current department and all its children are accessible for plist, should be inserted into sec table
    # T - current department is not accessible for plist, so should be deleted from sec table
    # NC- current departments and all its children are not accessible for plist, should be deleted from sec table
    """

    tenant_id = kwargs.get('tenant_id', 0)
    plist = kwargs.get('plist', None)
    by_plist = update_all = False
    if plist:
        by_plist = True
    else:
        # update all row security data
        update_all = True

    result_dic = {}

    conn = self.conn
    trans = conn.begin()
    conn.execute("SET autocommit= 0 ")
    try:
        cls_sec_dept = self.get_table('hhr_data_sec_plist_dept')
        end_dt = get_current_dttm()

        # 判断开关“部门权限包含未来”，若不是Y，根据当前最新的部门树刷新。若是Y，还需要合并未来所有部门树。
        dept_future_sw = get_cfg_sw_val(self.conn, tenant_id, 'OM_DEPT_FUTURE')
        # ------------------update department security table-------------------------#

        # purge all original department values for each tenant (and plist if provided)
        if by_plist:
            conn.execute(cls_sec_dept.delete().
                         where(cls_sec_dept.c.tenant_id == tenant_id).
                         where(cls_sec_dept.c.hhr_plist_code == plist))
        elif update_all:
            conn.execute(cls_sec_dept.delete().where(cls_sec_dept.c.tenant_id == tenant_id))

        stmt2 = ''
        if by_plist:
            stmt2 = text(
                "select hhr_plist_code, hhr_dept_code from boogoo_foundation.hhr_permission_dept where tenant_id = :b1 "
                "and hhr_subdept_option = :b2 and hhr_plist_code = :b3 order by 1, 2")
        elif update_all:
            stmt2 = text(
                "select hhr_plist_code, hhr_dept_code from boogoo_foundation.hhr_permission_dept where tenant_id = :b1 "
                "and hhr_subdept_option = :b2 order by 1, 2")

        # # Type C - get class departments configuration data with type C
        # current department and all its children are accessible for plist, should be inserted into sec table
        rs2 = conn.execute(stmt2, b1=tenant_id, b2='C', b3=plist).fetchall()
        csd_ins = cls_sec_dept.insert()
        for row2 in rs2:
            class_code = row2['hhr_plist_code']
            dept_id = row2['hhr_dept_code']
            # get children departments for current dept
            result_key = str(tenant_id) + "_" + str(end_dt) + "_" + str(dept_id) + "_" + str(dept_future_sw)
            if result_key in result_dic:
                result_lst = result_dic[result_key]
            else:
                result_lst = get_child_depts(tenant_id, end_dt, dept_id, dept_future_sw, dba=self)
                result_dic[result_key] = result_lst

            for child_dict in result_lst:
                child_list = child_dict['children']
                effdt = child_dict['effdt']
                values = []
                try:
                    for c in range(len(child_list)):
                        v_d = {'tenant_id': tenant_id,
                               'hhr_plist_code': class_code,
                               'hhr_dept_code': child_list[c],
                               'hhr_efft_date': effdt}
                        values.append(v_d)
                    if len(values) > 0:
                        conn.execute(csd_ins, values)
                except IntegrityError:
                    pass

        # # Type T - get class departments configuration data with type T
        # current department is not accessible for plist, so should be deleted from sec table\
        rs2 = conn.execute(stmt2, b1=tenant_id, b2='T', b3=plist).fetchall()
        for row2 in rs2:
            class_code = row2['hhr_plist_code']
            dept_id = row2['hhr_dept_code']

            # delete current department from sec table for this plist
            conn.execute(cls_sec_dept.delete().
                         where(cls_sec_dept.c.tenant_id == tenant_id).
                         where(cls_sec_dept.c.hhr_plist_code == class_code).
                         where(cls_sec_dept.c.hhr_dept_code == dept_id))

        # # Type NC - get class departments configuration data with type NC
        # current departments and all its children are not accessible for plist, should be deleted from sec table
        rs2 = conn.execute(stmt2, b1=tenant_id, b2='NC', b3=plist).fetchall()
        for row2 in rs2:
            class_code = row2['hhr_plist_code']
            dept_id = row2['hhr_dept_code']

            # get children departments for current dept
            result_key = str(tenant_id) + "_" + str(end_dt) + "_" + str(dept_id) + "_" + str(dept_future_sw)
            if result_key in result_dic:
                result_lst = result_dic[result_key]
            else:
                result_lst = get_child_depts(tenant_id, end_dt, dept_id, dept_future_sw, dba=self)
                result_dic[result_key] = result_lst

            for child_dict in result_lst:
                child_list = child_dict['children']
                # delete current department and all its children from sec table for this plist
                for d in range(len(child_list)):
                    conn.execute(cls_sec_dept.delete().
                                 where(cls_sec_dept.c.tenant_id == tenant_id).
                                 where(cls_sec_dept.c.hhr_plist_code == class_code).
                                 where(cls_sec_dept.c.hhr_dept_code == child_list[d]))

        trans.commit()

    except SoftTimeLimitExceeded:
        trans.rollback()
        print('用户撤销了进程或者超过了Soft time限制')
    except Exception:
        trans.rollback()
        # log_file = self.create_file(self, 'class_dept_upd.log')
        # log_file.write_line(traceback.format_exc())
        # log_file.close()
        raise
    finally:
        conn.close()
    return 1
