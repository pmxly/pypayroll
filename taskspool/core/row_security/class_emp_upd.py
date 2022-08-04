# -*- coding: utf-8 -*-

from ....celeryapp import app
from celery.utils.log import get_task_logger
from ....dbtask import DatabaseTask
from celery.exceptions import SoftTimeLimitExceeded
import traceback
from sqlalchemy import text
from ....utils import get_current_dttm
from ....hr.bus_utils import get_cfg_sw_val
from datetime import datetime

logger = get_task_logger(__name__)


@app.task(name='class_emp_upd', base=DatabaseTask, bind=True)
def class_emp_upd(self, **kwargs):
    """
    Desc: 刷新人员行安全性
    Author: David
    Date: 2019/03/11
    """

    tenant_id = kwargs.get('tenant_id', 0)

    # update by one person (synchronously)
    emplid = kwargs.get('emplid', None)
    empl_rcd = kwargs.get('empl_rcd', None)

    # update by persons list (asynchronously)
    emp_string = kwargs.get('emp_string', None)

    by_emp = by_emp_list = by_all_emp = False

    if (emplid is not None) and (empl_rcd is not None):
        by_emp = True
    elif emp_string is not None and emp_string.strip() != '':
        if emp_string == 'ALL':
            by_all_emp = True
        else:
            by_emp_list = True
    try:
        data_sec_pers_dept = self.get_table('hhr_data_sec_pers_dept')
        data_sec_pers_pygrp = self.get_table('hhr_data_sec_pers_pygrp')
        end_dt = get_current_dttm()

        # 判断人员任职权限（所属部门）包含未来
        job_future_sw = get_cfg_sw_val(self.conn, tenant_id, 'PA_JOB_FUTURE')
        # 判断人员薪资权限（所属薪资组）包含未来
        pg_future_sw = get_cfg_sw_val(self.conn, tenant_id, 'PY_PG_FUTURE')

        trans = self.conn.begin()
        try:
            # ------------------update employees security table-------------------------#

            # Purge all original value for each sys code (and person if provided)
            if by_emp:
                self.conn.execute(data_sec_pers_dept.delete()
                                  .where(data_sec_pers_dept.c.tenant_id == tenant_id)
                                  .where(data_sec_pers_dept.c.hhr_empid == emplid)
                                  .where(data_sec_pers_dept.c.hhr_emp_rcd == empl_rcd))
                self.conn.execute(data_sec_pers_pygrp.delete()
                                  .where(data_sec_pers_pygrp.c.tenant_id == tenant_id)
                                  .where(data_sec_pers_pygrp.c.hhr_empid == emplid)
                                  .where(data_sec_pers_pygrp.c.hhr_emp_rcd == empl_rcd))
            elif by_all_emp:
                self.conn.execute(data_sec_pers_dept.delete().where(data_sec_pers_dept.c.tenant_id == tenant_id))
                self.conn.execute(data_sec_pers_pygrp.delete().where(data_sec_pers_pygrp.c.tenant_id == tenant_id))

            if by_emp or by_all_emp:
                # 更新hhr_data_sec_pers_dept表
                load_data_sec_pers_dept(self.conn, data_sec_pers_dept, tenant_id, emplid, empl_rcd, end_dt, job_future_sw)
                # 更新hhr_data_sec_pers_pygrp表
                load_cls_sec_py_grp(self.conn, data_sec_pers_pygrp, tenant_id, emplid, empl_rcd, end_dt, pg_future_sw)

            if by_emp_list:
                emps_list = emp_string.split(',')
                del_pers_sql = text("delete from boogoo_foundation.hhr_data_sec_pers_dept where tenant_id = :b1 and hhr_empid = :b2 ")
                del_py_grp_sql = text("delete from boogoo_foundation.hhr_data_sec_pers_pygrp where tenant_id = :b1 and hhr_empid = :b2 ")
                emp_rcd_sql = text("select distinct hhr_emp_rcd from boogoo_corehr.hhr_org_per_jobdata where tenant_id = :b1 and hhr_empid = :b2 ")

                for each_emp_id in emps_list:
                    each_emp_id = each_emp_id.strip()
                    self.conn.execute(del_pers_sql, b1=tenant_id, b2=each_emp_id)
                    self.conn.execute(del_py_grp_sql, b1=tenant_id, b2=each_emp_id)
                    emp_rcd_rs = self.conn.execute(emp_rcd_sql, b1=tenant_id, b2=each_emp_id).fetchall()
                    for emp_rcd_row in emp_rcd_rs:
                        each_emp_rcd = emp_rcd_row['hhr_emp_rcd']
                        load_data_sec_pers_dept(self.conn, data_sec_pers_dept, tenant_id, each_emp_id, each_emp_rcd, end_dt, job_future_sw)
                        load_cls_sec_py_grp(self.conn, data_sec_pers_pygrp, tenant_id, each_emp_id, each_emp_rcd, end_dt, pg_future_sw)
            trans.commit()
        except Exception:
            trans.rollback()
            raise

    except SoftTimeLimitExceeded:
        print('用户撤销了进程或者超过了Soft time限制')
    except Exception:
        log_file = self.create_file(self, 'class_emp_upd.log')
        log_file.write_line(traceback.format_exc())
        log_file.close()
        raise
    finally:
        self.conn.close()
    return 1


def get_pers_dept_info(conn, tenant_id, emp_id, emp_rcd, end_dt, job_future_sw):
    s_per = "SELECT j.hhr_empid, j.hhr_emp_rcd, j.hhr_efft_date, j.hhr_efft_seq, j.hhr_dept_code, j.hhr_company_code, j.hhr_status " + \
            "FROM boogoo_corehr.hhr_org_per_jobdata j WHERE j.tenant_id = :b1 " + \
            "AND ( " \
            "j.hhr_efft_date = " + \
            "   (SELECT MAX(j1.hhr_efft_date) FROM boogoo_corehr.hhr_org_per_jobdata j1 where j1.tenant_id = j.tenant_id " + \
            "    and j1.hhr_empid = j.hhr_empid and j1.hhr_emp_rcd = j.hhr_emp_rcd and j1.hhr_efft_date <= :b2) " \
            " or j.hhr_efft_date > :b3 " \
            ") " \
            "AND j.hhr_efft_seq = " + \
            "   (SELECT MAX(j2.hhr_efft_seq) FROM boogoo_corehr.hhr_org_per_jobdata j2 WHERE j2.tenant_id = j.tenant_id " + \
            "    and j2.hhr_empid = j.hhr_empid and j2.hhr_emp_rcd = j.hhr_emp_rcd and j2.hhr_efft_date = j.hhr_efft_date) "
    if (emp_id is not None) and (emp_rcd is not None):
        s_per = s_per + ' AND j.HHR_EMPID = :b4 AND j.HHR_EMP_RCD = :b5 '
    s_per = text(s_per)

    if job_future_sw == 'Y':
        temp_end_dt = end_dt
    else:
        temp_end_dt = datetime(9999, 12, 31).date()
    rs_per = conn.execute(s_per, b1=tenant_id, b2=end_dt, b3=temp_end_dt, b4=emp_id, b5=emp_rcd).fetchall()
    return rs_per


def get_pers_py_grp_info(conn, tenant_id, emp_id, emp_rcd, end_dt, pg_future_sw):
    s_grp = "SELECT j.hhr_empid, j.hhr_emp_rcd, j.hhr_efft_date, j.hhr_efft_seq, j.hhr_pygroup_id, j.hhr_maintn_si_phf " + \
            "FROM boogoo_payroll.hhr_py_assign_pg j WHERE j.tenant_id = :b1 " + \
            "AND ( " \
            "j.hhr_efft_date = " + \
            "   (SELECT MAX(j1.hhr_efft_date) FROM boogoo_payroll.hhr_py_assign_pg j1 WHERE j1.tenant_id = j.tenant_id " + \
            "    and j1.hhr_empid = j.hhr_empid and j1.hhr_emp_rcd = j.hhr_emp_rcd and j1.hhr_efft_date <= :b2) " \
            " or j.hhr_efft_date > :b3 " \
            ") " \
            "AND j.hhr_efft_seq = " + \
            "   (SELECT MAX(j2.hhr_efft_seq) FROM boogoo_payroll.hhr_py_assign_pg j2 WHERE j2.tenant_id = j.tenant_id " + \
            "    and j2.hhr_empid = j.hhr_empid and j2.hhr_emp_rcd = j.hhr_emp_rcd and j2.hhr_efft_date = j.hhr_efft_date) "
    if (emp_id is not None) and (emp_rcd is not None):
        s_grp = s_grp + ' and j.hhr_empid = :b4 and j.hhr_emp_rcd = :b5 '
    s_grp = text(s_grp)

    if pg_future_sw == 'Y':
        temp_end_dt = end_dt
    else:
        temp_end_dt = datetime(9999, 12, 31).date()
    rs_grp = conn.execute(s_grp, b1=tenant_id, b2=end_dt, b3=temp_end_dt, b4=emp_id, b5=emp_rcd).fetchall()
    return rs_grp


def load_data_sec_pers_dept(conn, tab_obj, tenant_id, emp_id, emp_rcd, end_dt, job_future_sw):
    tab_ins = tab_obj.insert()
    rs_per = get_pers_dept_info(conn, tenant_id, emp_id, emp_rcd, end_dt, job_future_sw)
    for row_per in rs_per:
        new_emp_id = row_per['hhr_empid']
        new_emp_rcd = row_per['hhr_emp_rcd']
        values = [
            {'tenant_id': tenant_id,
             'hhr_empid': new_emp_id,
             'hhr_emp_rcd': new_emp_rcd,
             'hhr_efft_date': row_per['hhr_efft_date'],
             'hhr_efft_seq': row_per['hhr_efft_seq'],
             'hhr_dept_code': row_per['hhr_dept_code'],
             'hhr_company_code': row_per['hhr_company_code'],
             'hhr_status': row_per['hhr_status']
             }
        ]
        conn.execute(tab_ins, values)


def load_cls_sec_py_grp(conn, tab_obj, tenant_id, emp_id, emp_rcd, end_dt, pg_future_sw):
    tab_ins = tab_obj.insert()
    rs_grp = get_pers_py_grp_info(conn, tenant_id, emp_id, emp_rcd, end_dt, pg_future_sw)
    for row_grp in rs_grp:
        new_emp_id = row_grp['hhr_empid']
        new_emp_rcd = row_grp['hhr_emp_rcd']
        values = [
            {'tenant_id': tenant_id,
             'hhr_empid': new_emp_id,
             'hhr_emp_rcd': new_emp_rcd,
             'hhr_efft_date': row_grp['hhr_efft_date'],
             'hhr_efft_seq': row_grp['hhr_efft_seq'],
             'hhr_pygroup_id': row_grp['hhr_pygroup_id'],
             'hhr_maintn_si_phf': row_grp['hhr_maintn_si_phf']
             }
        ]
        conn.execute(tab_ins, values)
