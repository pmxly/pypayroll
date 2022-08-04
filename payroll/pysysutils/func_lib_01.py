# coding:utf-8
"""
算薪中的公共函数
create by wangling 2018/7/3
"""

from ..pysysutils import global_variables as gv
from ..pycalendar.calender import Calendar
from ...utils import get_current_dttm
from sqlalchemy import select
from sqlalchemy.sql import text
from sqlalchemy.exc import IntegrityError
from datetime import date


# from ..pyexecute.parameterentity import EmpParameter


def ins_py_cal_task(db, tenant_id, cal_grp_id, cal_id, run_ctl, task_name, action_user, prcs_num):
    """插入薪资计算对应的运行控制ID"""

    task = db.get_table('hhr_py_cal_task', schema_name='boogoo_payroll')
    ins = task.insert()
    values = [
        {'tenant_id': tenant_id,
         'hhr_pycalgrp_id': cal_grp_id,
         'hhr_py_cal_id': cal_id,
         'hhr_runctl_id': run_ctl,
         'hhr_task_name': task_name,
         'hhr_prcs_num': prcs_num,
         'hhr_create_dttm': get_current_dttm(),
         'hhr_create_user': action_user
         }]
    try:
        db.conn.execute(ins, values)
    except IntegrityError:
        raise Exception("ins_py_cal_task出错")


def del_py_cal_task(db, tenant_id, cal_grp_id, cal_id, task_name=None):
    """删除薪资计算运行控制ID日志记录"""

    if task_name:
        sql = text("delete from boogoo_payroll.hhr_py_cal_task where tenant_id = :b1 and hhr_pycalgrp_id = :b2 "
                   "and hhr_py_cal_id = :b3 and hhr_task_name = :b4 ")
        db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=cal_id, b4=task_name)
    else:
        sql = text(
            "delete from boogoo_payroll.hhr_py_cal_task where tenant_id = :b1 and hhr_pycalgrp_id = :b2 and hhr_py_cal_id = :b3")
        db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=cal_id)


def ins_py_calc_stat(**kwargs):
    """
    Desc: 插入薪资标记&计算结果
    Author: David
    Date: 2018/08/15
    """
    try:
        db = gv.get_db()
        stat = db.get_table('hhr_py_payee_calc_stat', 'boogoo_payroll')

        """20210524修改：
        在表hhr_py_payee_calc_stat中增加3个字段：hhr_company_code、hhr_dept_code、hhr_posn_code；
        在标记人员的时候记录员工所属的公司、组织、岗位；
        取员工任职的逻辑：生效日期根据配置维护HHR.PY.POST.MAIN_COST_DATE中维护的日期（当前维护的是10，计算5月工资则日期为2021-05-10）；
        若未取到任何记录，则取大于此日期记录中的生效日期最小的（如2021-05-10之后才入职的员工）。
        """
        stmt = text("select A.hhr_company_code, A.hhr_dept_code, A.hhr_posn_code from boogoo_corehr.hhr_org_per_jobdata A "
                    "where a.tenant_id = :b1 and :b2 between a.hhr_efft_date and a.hhr_efft_end_date "
                    "and a.hhr_efft_seq = (select max(a1.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata a1 where a1.tenant_id = a.tenant_id "
                    "and a1.hhr_empid=a.hhr_empid and a1.hhr_emp_rcd = a.hhr_emp_rcd and a1.hhr_efft_date = a.hhr_efft_date) "
                    "and a.hhr_empid = :b3 and a.hhr_emp_rcd = :b4 ")

        cal = kwargs['cal']
        main_cost_day = cal.main_cost_day
        prd_end_dt = cal.period_cal_entity.end_date
        year = prd_end_dt.year
        month = prd_end_dt.month
        end_dt = date(year, month, main_cost_day)

        r = db.conn.execute(stmt, b1=kwargs['tenant_id'], b2=end_dt, b3=kwargs['emp_id'],
                            b4=kwargs['emp_rcd']).fetchone()
        if r is None:
            stmt2 = text(
                "select A.hhr_company_code, A.hhr_dept_code, A.hhr_posn_code from boogoo_corehr.hhr_org_per_jobdata A "
                "where a.tenant_id = :b1 and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 and a.hhr_efft_date > :b4 order by a.hhr_efft_date ")
            r = db.conn.execute(stmt2, b1=kwargs['tenant_id'], b2=kwargs['emp_id'], b3=kwargs['emp_rcd'],
                                b4=end_dt).fetchone()
        if r is None:
            company_cd = ''
            dept_cd = ''
            posn_cd = ''
        else:
            company_cd = '' if r['hhr_company_code'] is None else r['hhr_company_code']
            dept_cd = '' if r['hhr_dept_code'] is None else r['hhr_dept_code']
            posn_cd = '' if r['hhr_posn_code'] is None else r['hhr_posn_code']

        ins = stat.insert()
        values = [
            {'tenant_id': kwargs['tenant_id'],
             'hhr_pycalgrp_id': kwargs['cal_grp_id'],
             'hhr_py_cal_id': kwargs['cal_id'],
             'hhr_empid': kwargs['emp_id'],
             'hhr_emp_rcd': kwargs['emp_rcd'],
             'hhr_upt_calid': kwargs.get('upd_cal_id', ''),
             'hhr_lock_flg': kwargs['lock'],
             'hhr_py_ind_stat': kwargs['ind_stat'],
             'hhr_py_calc_stat': kwargs['calc_stat'],
             'hhr_company_code': company_cd,
             'hhr_dept_code': dept_cd,
             'hhr_posn_code': posn_cd,
             'hhr_create_dttm': get_current_dttm(),
             'hhr_create_user': kwargs['user_id'],
             'hhr_modify_dttm': get_current_dttm(),
             'hhr_modify_user': kwargs['user_id']
             }]
        db.conn.execute(ins, values)
    except IntegrityError:
        pass


def update_py_calc_stat(**kwargs):
    """更新人员薪资计算结果"""

    db = kwargs.get('db', None)
    tenant_id = kwargs.get('tenant_id', 0)
    cal_grp_id = kwargs.get('cal_grp_id', None)
    emp_id = kwargs.get('emp_id', None)
    emp_rcd = kwargs.get('emp_rcd', None)
    status = kwargs.get('status', None)
    user = kwargs.get('user', None)

    sql_stat = text(
        "update boogoo_payroll.hhr_py_payee_calc_stat set hhr_py_calc_stat = :b5, hhr_modify_dttm = :b6, hhr_modify_user = :b7 "
        "where tenant_id = :b1 and hhr_pycalgrp_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    db.conn.execute(sql_stat, b1=tenant_id, b2=cal_grp_id, b3=emp_id, b4=emp_rcd, b5=status, b6=get_current_dttm(),
                    b7=user)


def ins_py_calc_msg(**kwargs):
    """
    Desc: 插入薪资标记&计算消息
    Author: David
    Date: 2018/08/15
    """

    db = gv.get_db()
    msg = db.get_table('hhr_py_payee_calc_msg', 'boogoo_payroll')
    tenant_id = kwargs['tenant_id']
    cal_grp_id = kwargs['cal_grp_id']
    cal_id = kwargs['cal_id']
    f_cal_id = kwargs.get('f_cal_id', '')
    emp_id = kwargs['emp_id']
    emp_rcd = kwargs['emp_rcd']
    msg_class = kwargs['msg_class']
    sql = text(
        "select max(hhr_seq_num) from boogoo_payroll.hhr_py_payee_calc_msg where tenant_id = :b1 and hhr_pycalgrp_id = :b2 "
        "and hhr_py_cal_id = :b3 and hhr_empid = :b4 and hhr_emp_rcd = :b5 and hhr_py_msg_class = :b6 ")
    row = db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=cal_id, b4=emp_id, b5=emp_rcd, b6=msg_class).fetchone()
    if row[0] is not None:
        new_seq = row[0] + 1
    else:
        new_seq = 1

    msg_type = kwargs['msg_type']
    msg_txt = kwargs['msg_txt']
    user_id = kwargs['user_id']
    cur_dtt = get_current_dttm()

    # msg_in_sql = text("insert into HHR_PAYEE_CALC_MSG(tenant_id, HHR_PYCALGRP_ID, HHR_PY_CAL_ID, HHR_EMPID, HHR_EMP_RCD, "
    #                   "HHR_PY_MSG_CLASS, HHR_SEQ_NUM, HHR_F_CAL_ID, HHR_PY_MSG_TYPE, HHR_MSG_TXT, HHR_CREATE_DTTM, HHR_CREATE_USER, "
    #                   "HHR_MODIFY_DTTM, HHR_MODIFY_USER) values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10, :b11, :b12, :b13, :b14) ")
    # db.conn.execute(msg_in_sql, b1=tenant_id, b2=cal_grp_id, b3=cal_id, b4=emp_id, b5=emp_rcd, b6=msg_class, b7=new_seq, b8='',
    #                 b9=msg_type, b10=msg_txt, b11=cur_dttm, b12=user_id, b13=cur_dttm, b14=user_id)

    ins = msg.insert()
    values = [
        {'tenant_id': tenant_id,
         'hhr_pycalgrp_id': cal_grp_id,
         'hhr_py_cal_id': cal_id,
         'hhr_empid': emp_id,
         'hhr_emp_rcd': emp_rcd,
         'hhr_py_msg_class': msg_class,
         'hhr_seq_num': new_seq,
         'hhr_f_cal_id': f_cal_id,
         'hhr_py_msg_type': msg_type,
         'hhr_msg_txt': msg_txt,
         'hhr_create_dttm': cur_dtt,
         'hhr_create_user': user_id,
         'hhr_modify_dttm': cur_dtt,
         'hhr_modify_user': user_id
         }
    ]
    db.conn.execute(ins, values)


def get_cal_obj_in_dev(tenant_id, cal_id):
    """
    在已经实例化好的日历字段中获取日历对象，如果没有，新添加一个
    :param tenant_id:租户ID
    :param cal_id:日历ID
    :return:
    """
    calendar_dic = gv.get_run_var_value('CAL_OBJ_DIC')
    if cal_id in calendar_dic:
        return calendar_dic[cal_id]
    else:
        calendar_dic[cal_id] = Calendar(tenant_id, cal_id)
    pass


def get_run_payees(run_param):
    """
    Desc: 获取需要计算薪资的人员任职记录
    Author: David
    Date: 2018/08/21
    """

    # 存储需要记录过程日志的人员任职记录
    log_emp_rec_tup_lst = []
    if run_param['log_flag'] == 'Y':
        log_emp_rec_tup_lst = get_emp_rec_tup_lst(run_param.get('log_emp_rec_str', ''))

    # 指定计算的人员
    emp_rec_str = run_param.get('emp_rec_str', '')

    task_id = run_param.get('task_id', '')
    if task_id != '':
        db = gv.get_db()
    else:
        db = gv.get_flask_db()
    tenant_id = run_param['tenant_id']
    cal_grp_id = run_param['cal_grp_id']
    emp_rec_cond = get_emp_rec_cond(emp_rec_str)
    stat_str = "select tenant_id, hhr_pycalgrp_id, hhr_py_cal_id, hhr_upt_calid, hhr_empid, hhr_emp_rcd from boogoo_payroll.hhr_py_payee_calc_stat " \
               "where tenant_id = :b1 and hhr_pycalgrp_id = :b2 and hhr_lock_flg = '1' and hhr_py_ind_stat = 'S' "
    stat_str = stat_str + emp_rec_cond
    rs = db.conn.execute(text(stat_str), b1=tenant_id, b2=cal_grp_id).fetchall()
    payees = []
    for row in rs:
        emp_dic = dict()
        emp_dic['tenant_id'] = row['tenant_id']
        emp_dic['cal_grp_id'] = row['hhr_pycalgrp_id']
        emp_dic['cal_id'] = row['hhr_py_cal_id']
        emp_dic['upd_cal_id'] = row['hhr_upt_calid']
        emp_dic['emp_id'] = row['hhr_empid']
        emp_dic['emp_rcd'] = row['hhr_emp_rcd']

        if run_param['log_flag'] == 'Y':
            # 判断该人员任职记录是否需要记录过程日志
            if (emp_dic['emp_id'], emp_dic['emp_rcd']) in log_emp_rec_tup_lst:
                emp_dic['emp_log_flag'] = 'Y'
            else:
                emp_dic['emp_log_flag'] = 'N'
        else:
            emp_dic['emp_log_flag'] = 'N'

        payees.append(emp_dic)
    return payees


def chunk_payees(payees, size):
    """
    Desc: 将需要计算薪资的人员进行拆分
    Author: David
    Date: 2018/08/21
    """
    for i in range(0, len(payees), size):
        yield payees[i:i + size]


def payee_pay_group_check(payee, cal, bgn_dt=None, end_dt=None):
    """
    Desc: 校验受款人在期间结束日期内的薪资组是否与所在日历薪资组一致
    Author: David
    Date: 2018/08/14
    """

    db = gv.get_db()
    tenant_id = payee.tenant_id
    emp_id = payee.emp_id
    emp_rcd = payee.emp_rcd

    if bgn_dt is None and end_dt is None:
        prd_end_dt = cal.period_cal_entity.end_date
    else:
        prd_end_dt = end_dt
    py_grp_id = cal.py_group_id
    t1 = text(
        "select a.hhr_pygroup_id from boogoo_payroll.hhr_py_assign_pg a where a.tenant_id = :b1 and a.hhr_efft_date <= :b2 "
        " and a.hhr_empid = :b3 and hhr_emp_rcd = :b4 order by hhr_efft_date desc, hhr_efft_seq desc ")
    r = db.conn.execute(t1, b1=tenant_id, b2=prd_end_dt, b3=emp_id, b4=emp_rcd).fetchone()
    if r is not None:
        emp_py_grp = r['hhr_pygroup_id']
        if emp_py_grp != py_grp_id:
            check = False
        else:
            check = True
    else:
        check = False
    return check


def del_py_calc_rslt(db, tenant_id, cal_grp_id, user_id):
    """
    删除人员薪资计算结果
    :param db: DataBaseAlchemy对象
    :param tenant_id: 租户ID
    :param cal_grp_id: 日历组ID
    :param user_id: 用户ID
    :return: None
    """

    exists = False
    # 清除/更新人员薪资计算目录数据
    t = text("select tenant_id, hhr_empid, hhr_emp_rcd, hhr_seq_num, hhr_hist_seq, hhr_upd_seq "
             "from boogoo_payroll.hhr_py_cal_catalog where tenant_id =:b1 and hhr_pycalgrp_id = :b2")
    rs = db.conn.execute(t, b1=tenant_id, b2=cal_grp_id).fetchall()

    # 需要更新的记录：所在期日历组为当前日历组的记录中，历史序号列记录的序号对应的记录。从P或者O(历史)还原为A(活动)
    t1 = text(
        "update boogoo_payroll.hhr_py_cal_catalog A set A.hhr_py_rec_stat = 'A', A.hhr_modify_dttm = :b5, A.hhr_modify_user = :b6 "
        "where (A.hhr_py_rec_stat = 'P' or A.hhr_py_rec_stat = 'O') and A.tenant_id = :b1 and A.hhr_empid = :b2 and A.hhr_emp_rcd = :b3 and A.hhr_seq_num = :b4 ")
    # 清除薪资结果-变量
    t2 = text(
        "delete from boogoo_payroll.hhr_py_rslt_var where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-期间分段
    t3 = text(
        "delete from boogoo_payroll.hhr_py_rslt_seg where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-任职
    t4 = text(
        "delete from boogoo_payroll.hhr_py_rslt_job where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-工作日历
    t5 = text(
        "delete from boogoo_payroll.hhr_py_rslt_wkcal where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-分段因子hhr_sal_plan_cd
    t6 = text(
        "delete from boogoo_payroll.hhr_py_rslt_segfact where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-汇率
    # t7 = text("delete from HHR_PY_RSLT_EXRATE where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-考勤记录
    t8 = text(
        "delete from boogoo_payroll.hhr_py_rslt_abs where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-薪资项目
    t9 = text(
        "delete from boogoo_payroll.hhr_py_rslt_pin where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-薪资项目累计
    t10 = text(
        "delete from boogoo_payroll.hhr_py_rslt_accm where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-累计日历
    # t11 = text("delete from HHR_PY_RSLT_ACCM_CAL where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除可写数组
    t12 = text(
        "delete from boogoo_payroll.hhr_py_rslt_array_01 where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    # 清除薪资结果-薪资项目子类
    t13 = text(
        "delete from boogoo_payroll.hhr_py_rslt_pin_sub_cls where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    for row in rs:
        exists = True
        emp_id = row['hhr_empid']
        emp_rcd = row['hhr_emp_rcd']
        seq_num = row['hhr_seq_num']
        hist_seq = row['hhr_hist_seq']
        upd_seq = row['hhr_upd_seq']
        if upd_seq:
            hist_seq = upd_seq
        if hist_seq:
            db.conn.execute(t1, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=hist_seq, b5=get_current_dttm(), b6=user_id)
        db.conn.execute(t2, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        db.conn.execute(t3, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        db.conn.execute(t4, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        db.conn.execute(t5, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        db.conn.execute(t6, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        # db.conn.execute(t7, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        db.conn.execute(t8, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        db.conn.execute(t9, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        db.conn.execute(t10, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        # db.conn.execute(t11, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)

        db.conn.execute(t12, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)
        db.conn.execute(t13, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num)

    if exists:
        # t1 = db.conn.begin() 2021-03-11
        # 删除所在期日历组为当前日历组的记录
        t13 = text("delete from boogoo_payroll.hhr_py_cal_catalog where tenant_id =:b1 and hhr_pycalgrp_id = :b2 ")
        db.conn.execute(t13, b1=tenant_id, b2=cal_grp_id)
        # t1.commit() 2021-03-11


def del_py_calc_rslt_obsolete(db, tenant_id, cal_grp_id, user_id):
    """
    删除人员薪资计算结果(废弃)
    :param db: DataBaseAlchemy对象
    :param tenant_id: 租户ID
    :param cal_grp_id: 日历组ID
    :param user_id: 用户ID
    :return: None
    """

    """清除/更新人员薪资计算目录数据"""

    # 需要更新的记录：所在期日历组为当前日历组的记录中，历史序号列记录的序号对应的记录。从P或者O(历史)还原为A(活动)
    t1 = text(
        "Update boogoo_payroll.hhr_py_cal_catalog a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd "
        "and a.hhr_seq_num = (case when c.hhr_upd_seq is not null then c.hhr_upd_seq else c.hhr_hist_seq end) "
        "set a.hhr_py_rec_stat = 'A', a.hhr_modify_dttm = :b3, a.hhr_modify_user = :b4 "
        "where(a.hhr_py_rec_stat = 'P' or a.hhr_py_rec_stat = 'O') "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除薪资结果-变量
    t2 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_var a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除薪资结果-期间分段
    t3 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_seg a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除薪资结果-任职
    t4 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_job a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除薪资结果-工作日历
    t5 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_wkcal a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除薪资结果-分段因子
    t6 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_segfact a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除薪资结果-考勤记录
    t7 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_abs a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除薪资结果-薪资项目
    t8 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_pin a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除薪资结果-薪资项目累计
    t9 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_accm a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")
    # 清除可写数组
    t10 = text(
        "delete a from boogoo_payroll.hhr_py_rslt_array_01 a join boogoo_payroll.hhr_py_cal_catalog c on a.tenant_id = c.tenant_id "
        "and a.hhr_empid = c.hhr_empid and a.hhr_emp_rcd = c.hhr_emp_rcd and a.hhr_seq_num = c.hhr_seq_num "
        "and c.tenant_id =:b1 and c.hhr_pycalgrp_id = :b2 ")

    db.conn.execute(t1, b1=tenant_id, b2=cal_grp_id, b3=get_current_dttm(), b4=user_id)
    db.conn.execute(t2, b1=tenant_id, b2=cal_grp_id)
    db.conn.execute(t3, b1=tenant_id, b2=cal_grp_id)
    db.conn.execute(t4, b1=tenant_id, b2=cal_grp_id)
    db.conn.execute(t5, b1=tenant_id, b2=cal_grp_id)
    db.conn.execute(t6, b1=tenant_id, b2=cal_grp_id)
    db.conn.execute(t7, b1=tenant_id, b2=cal_grp_id)
    db.conn.execute(t8, b1=tenant_id, b2=cal_grp_id)
    db.conn.execute(t9, b1=tenant_id, b2=cal_grp_id)
    db.conn.execute(t10, b1=tenant_id, b2=cal_grp_id)

    # 删除所在期日历组为当前日历组的记录
    t11 = text("delete from boogoo_payroll.hhr_py_cal_catalog where tenant_id =:b1 and hhr_pycalgrp_id = :b2 ")
    db.conn.execute(t11, b1=tenant_id, b2=cal_grp_id)


def del_py_calc_rslt_by_emp(db, tenant_id, cal_grp_id, user_id, in_emp_id, in_emp_rcd):
    """删除标记/计算结果、标记/计算消息表中当前日历对应的记录（仅薪资计算记录）"""
    s = text("select tenant_id, hhr_seq_num, hhr_hist_seq, hhr_upd_seq from boogoo_payroll.hhr_py_cal_catalog "
             "where tenant_id =:b1 and hhr_pycalgrp_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    rs = db.conn.execute(s, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd).fetchall()

    t1 = text(
        "update boogoo_payroll.hhr_py_cal_catalog a set A.hhr_py_rec_stat = 'A', a.hhr_modify_dttm = :b5, a.hhr_modify_user = :b6 "
        "where (a.hhr_py_rec_stat = 'P' or a.hhr_py_rec_stat = 'O') and a.tenant_id = :b1 and a.hhr_empid = :b2 and a.hhr_emp_rcd = :b3 and a.hhr_seq_num = :b4 ")
    t2 = text(
        "delete from boogoo_payroll.hhr_py_rslt_var where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t3 = text(
        "delete from boogoo_payroll.hhr_py_rslt_seg where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t4 = text(
        "delete from boogoo_payroll.hhr_py_rslt_job where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t5 = text(
        "delete from boogoo_payroll.hhr_py_rslt_wkcal where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t6 = text(
        "delete from boogoo_payroll.hhr_py_rslt_segfact where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t8 = text(
        "delete from boogoo_payroll.hhr_py_rslt_abs where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t9 = text(
        "delete from boogoo_payroll.hhr_py_rslt_pin where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t10 = text(
        "delete from boogoo_payroll.hhr_py_rslt_accm where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t12 = text(
        "delete from boogoo_payroll.hhr_py_rslt_array_01 where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")
    t13 = text(
        "delete from boogoo_payroll.hhr_py_rslt_pin_sub_cls where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_seq_num = :b4")

    for row in rs:
        seq_num = row['hhr_seq_num']
        hist_seq = row['hhr_hist_seq']
        upd_seq = row['hhr_upd_seq']
        if upd_seq:
            hist_seq = upd_seq
        if hist_seq:
            db.conn.execute(t1, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=hist_seq, b5=get_current_dttm(),
                            b6=user_id)
        db.conn.execute(t2, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t3, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t4, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t5, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t6, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t8, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t9, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t10, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t12, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)
        db.conn.execute(t13, b1=tenant_id, b2=in_emp_id, b3=in_emp_rcd, b4=seq_num)

    t13 = text("delete from boogoo_payroll.hhr_py_cal_catalog where tenant_id =:b1 and hhr_pycalgrp_id = :b2 "
               "and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    db.conn.execute(t13, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd)


def del_py_calc_log(db, tenant_id, cal_grp_id):
    """
    删除人员薪资计算过程日志
    :param db: DataBaseAlchemy对象
    :param tenant_id: 租户ID
    :param cal_grp_id: 日历组ID
    :return: None
    """

    log_tree_sql = text("delete from boogoo_payroll.hhr_py_log_tree where tenant_id = :b1 and hhr_py_cal_id = :b2")
    db.conn.execute(log_tree_sql, b1=tenant_id, b2=cal_grp_id)
    wt_log_sql = text("delete from boogoo_payroll.hhr_py_wt_log where tenant_id = :b1 and hhr_py_cal_id = :b2")
    db.conn.execute(wt_log_sql, b1=tenant_id, b2=cal_grp_id)
    wc_log_sql = text("delete from boogoo_payroll.hhr_py_wc_log where tenant_id = :b1 and hhr_py_cal_id = :b2")
    db.conn.execute(wc_log_sql, b1=tenant_id, b2=cal_grp_id)
    vr_log_sql = text("delete from boogoo_payroll.hhr_py_vr_log where tenant_id = :b1 and hhr_py_cal_id = :b2")
    db.conn.execute(vr_log_sql, b1=tenant_id, b2=cal_grp_id)
    fc_parm_sql = text("delete from boogoo_payroll.hhr_py_fc_param_log where tenant_id = :b1 and hhr_py_cal_id = :b2")
    db.conn.execute(fc_parm_sql, b1=tenant_id, b2=cal_grp_id)


def del_py_calc_log_by_emp(db, tenant_id, cal_grp_id, in_emp_id, in_emp_rcd):
    """删除人员薪资计算过程日志"""
    log_tree_sql = text(
        "delete from boogoo_payroll.hhr_py_log_tree where tenant_id = :b1 and hhr_py_cal_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    db.conn.execute(log_tree_sql, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd)
    wt_log_sql = text(
        "delete from boogoo_payroll.hhr_py_wt_log where tenant_id = :b1 and hhr_py_cal_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    db.conn.execute(wt_log_sql, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd)
    wc_log_sql = text(
        "delete from boogoo_payroll.hhr_py_wc_log where tenant_id = :b1 and hhr_py_cal_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    db.conn.execute(wc_log_sql, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd)
    vr_log_sql = text(
        "delete from boogoo_payroll.hhr_py_vr_log where tenant_id = :b1 and hhr_py_cal_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    db.conn.execute(vr_log_sql, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd)
    fc_parm_sql = text(
        "delete from boogoo_payroll.hhr_py_fc_param_log where tenant_id = :b1 and hhr_py_cal_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    db.conn.execute(fc_parm_sql, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd)


def get_emp_rec_tup_lst(emp_rec_str):
    emp_rec_tup_list = []
    if emp_rec_str:
        emp_rec_list = emp_rec_str.split(',')
        for emp_recs in emp_rec_list:
            pos = emp_recs.rfind('_')
            emp_id = "'" + str(emp_recs[:pos]) + "'"
            emp_rcd = emp_recs[pos + 1:]
            if emp_id is not None and emp_rcd is not None:
                emp_rec_tup_list.append((emp_id, int(emp_rcd)))
    return emp_rec_tup_list


def get_emp_rec_cond(emp_rec_str):
    emp_rec_cond = ""
    if emp_rec_str:
        emp_rec_tup_list = get_emp_rec_tup_lst(emp_rec_str)
        i = 0
        for emp_rec_tup in emp_rec_tup_list:
            i = i + 1
            if i == 1:
                emp_rec_cond += " and ( (hhr_empid, hhr_emp_rcd) in ((" + emp_rec_tup[0] + "," + str(
                    emp_rec_tup[1]) + ")) "
            else:
                emp_rec_cond += "or (hhr_empid, hhr_emp_rcd) in ((" + emp_rec_tup[0] + "," + str(emp_rec_tup[1]) + ")) "
        emp_rec_cond += " )"
    return emp_rec_cond


def del_payee_calc_stat(db, tenant_id, cal_grp_id, action_user, option, run_ctl_id='', emp_rec_str=None):
    """
    # 删除标记/计算结果表、标记/计算消息表中的对应记录
    :param db: DataBaseAlchemy对象
    :param tenant_id: 租户ID
    :param cal_grp_id: 日历组ID
    :param action_user: 操作用户
    :param option: PAY-只处理薪资计算记录； ALL-删除标记和薪资计算记录
    :param run_ctl_id: 运行控制ID（识别每次计算了哪些人员）
    :param emp_rec_str: 1001_1,1002_1,1003_1
    :return:
    """

    action_dttm = get_current_dttm()

    if option == 'ALL':
        sql_stat = text(
            "delete from boogoo_payroll.hhr_py_payee_calc_stat where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        sql_msg = text("delete from boogoo_payroll.hhr_py_payee_calc_msg where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        db.conn.execute(sql_stat, b1=tenant_id, b2=cal_grp_id)
        db.conn.execute(sql_msg, b1=tenant_id, b2=cal_grp_id)
    elif option == 'PAY':
        emp_rec_cond = ""
        if emp_rec_str:
            emp_rec_cond = get_emp_rec_cond(emp_rec_str)
        stat_str = "update boogoo_payroll.hhr_py_payee_calc_stat set hhr_py_calc_stat = '', hhr_modify_dttm = :b3, hhr_modify_user = :b4, " \
                   "hhr_runctl_id = :b5 where tenant_id = :b1 and hhr_pycalgrp_id = :b2 "
        stat_str = stat_str + emp_rec_cond
        sql_stat = text(stat_str)
        db.conn.execute(sql_stat, b1=tenant_id, b2=cal_grp_id, b3=action_dttm, b4=action_user, b5=run_ctl_id)

        msg_str = "delete from boogoo_payroll.hhr_py_payee_calc_msg where tenant_id = :b1 and hhr_pycalgrp_id = :b2 and hhr_py_msg_class = 'B' "
        msg_str = msg_str + emp_rec_cond
        sql_msg = text(msg_str)
        db.conn.execute(sql_msg, b1=tenant_id, b2=cal_grp_id)


def del_payee_calc_stat_by_emp(db, tenant_id, cal_grp_id, action_user, in_emp_id, in_emp_rcd):
    """删除人员薪资计算结果（多个表）/ 更新薪资计算目录"""
    action_dttm = get_current_dttm()
    sql_stat = text(
        "update boogoo_payroll.hhr_py_payee_calc_stat set hhr_py_calc_stat = '', hhr_modify_dttm = :b5, hhr_modify_user = :b6 "
        " where tenant_id = :b1 and hhr_pycalgrp_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    sql_msg = text(
        "delete from boogoo_payroll.hhr_py_payee_calc_msg where tenant_id = :b1 and hhr_pycalgrp_id = :b2 and hhr_empid = :b3 "
        "and hhr_emp_rcd = :b4 and hhr_py_msg_class = 'B' ")
    db.conn.execute(sql_stat, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd, b5=action_dttm, b6=action_user)
    db.conn.execute(sql_msg, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd)


def update_cal_grp_run(db, tenant_id, cal_grp_id, action_user, action_flag):
    """
    # 更新日历/日历组状态表
    :param db: DataBaseAlchemy对象
    :param tenant_id: 租户ID
    :param cal_grp_id: 日历组ID
    :param action_user: 操作用户
    :param action_flag: 操作标记
    :return:
    """

    action_dttm = get_current_dttm()

    if action_flag == 'PY_IDEN':
        sql = text(
            "update boogoo_payroll.hhr_py_cal_grp_run set hhr_cal_status = '3', hhr_idntfy_dttm = :b3, hhr_idntfy_user = :b4, "
            "hhr_modify_dttm = :b3, hhr_modify_user = :b4 where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=action_dttm, b4=action_user)
    if action_flag == 'CANCEL_IDEN':
        sql = text(
            "update boogoo_payroll.hhr_py_cal_grp_run set hhr_cal_status = '2', hhr_cancel_iden_dttm = :b3, hhr_cancel_iden_user = :b4, "
            "hhr_modify_dttm = :b3, hhr_modify_user = :b4 where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=action_dttm, b4=action_user)
    elif action_flag == 'PY_CALC' \
            or action_flag == 'RE_CALC' \
            or action_flag == 'RE_IDEN_CALC':
        sql = text(
            "update boogoo_payroll.hhr_py_cal_grp_run set hhr_cal_status = '4', hhr_calc_dttm = :b3, hhr_calc_user = :b4, "
            "hhr_modify_dttm = :b3, hhr_modify_user = :b4 where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=action_dttm, b4=action_user)
    elif action_flag == 'CANCEL_CALC':
        sql = text(
            "update boogoo_payroll.hhr_py_cal_grp_run set hhr_cal_status = '3', hhr_cancel_calc_dttm = :b3, hhr_cancel_calc_user = :b4, "
            "hhr_modify_dttm = :b3, hhr_modify_user = :b4 where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=action_dttm, b4=action_user)
    elif action_flag == 'CANCEL_BOTH':
        sql = text(
            "update boogoo_payroll.hhr_py_cal_grp_run set hhr_cal_status = '2', hhr_cancel_dttm = :b3, hhr_cancel_user = :b4, "
            "hhr_modify_dttm = :b3, hhr_modify_user = :b4 where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=action_dttm, b4=action_user)
    elif action_flag == 'FINISH':
        sql = text(
            "update boogoo_payroll.hhr_py_cal_grp_run set hhr_cal_status = '5', hhr_finish_dttm = :b3, hhr_finish_user = :b4, "
            "hhr_modify_dttm = :b3, hhr_modify_user = :b4 where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id, b3=action_dttm, b4=action_user)
    elif action_flag == 'CLOSE':
        sql_run = text(
            "update boogoo_payroll.hhr_py_cal_grp_run set hhr_cal_status = '8', hhr_close_dttm = :b3, hhr_close_user = :b4, "
            "hhr_modify_dttm = :b3, hhr_modify_user = :b4 where tenant_id = :b1 and hhr_pycalgrp_id = :b2")
        db.conn.execute(sql_run, b1=tenant_id, b2=cal_grp_id, b3=action_dttm, b4=action_user)


def update_run_status(**kwargs):
    db = kwargs['db']
    tenant_id = kwargs['tenant_id']
    cal_grp_id = kwargs['cal_grp_id']
    action_flag = kwargs['action_flag']
    action_user = kwargs.get('action_user', None)

    # 取消标记
    if action_flag == 'CANCEL_IDEN':
        # 将HHR_CAL_GRP_RUN状态置为2-新建
        update_cal_grp_run(db, tenant_id, cal_grp_id, action_user, action_flag)

        # 删除标记/计算结果、标记/计算消息表中当前日历对应的记录
        del_payee_calc_stat(db, tenant_id, cal_grp_id, action_user, 'ALL')

        # 删除人员薪资计算结果（多个表）
        del_py_calc_rslt(db, tenant_id, cal_grp_id, action_user)

        # 删除人员薪资计算过程日志
        del_py_calc_log(db, tenant_id, cal_grp_id)

        # 将人员追溯触发器表中“处理日历组”为当前日历组的“触发器状态”还原为U（未处理），并清空处理日历组
        u = text(
            "update boogoo_payroll.hhr_py_rto_trgr set hhr_trgr_status = 'U', hhr_pycalgrp_id = '' where tenant_id = :b1 and hhr_pycalgrp_id = :b2 ")
        db.conn.execute(u, b1=tenant_id, b2=cal_grp_id)

    # 取消计算
    elif action_flag == 'CANCEL_CALC':
        # 将HHR_CAL_GRP_RUN状态置为3-已标记
        update_cal_grp_run(db, tenant_id, cal_grp_id, action_user, action_flag)

        # 删除标记/计算结果、标记/计算消息表中当前日历对应的记录（仅薪资计算记录）
        del_payee_calc_stat(db, tenant_id, cal_grp_id, action_user, 'PAY')

        # 删除人员薪资计算结果（多个表）/ 更新薪资计算目录
        del_py_calc_rslt(db, tenant_id, cal_grp_id, action_user)

        # 删除人员薪资计算过程日志
        del_py_calc_log(db, tenant_id, cal_grp_id)

        # 将人员追溯触发器表中“处理日历组”为当前日历组的“触发器状态”还原为U（未处理），并清空处理日历组
        u = text(
            "update boogoo_payroll.hhr_py_rto_trgr set hhr_trgr_status = 'U', hhr_pycalgrp_id = '' where tenant_id = :b1 and hhr_pycalgrp_id = :b2 ")
        db.conn.execute(u, b1=tenant_id, b2=cal_grp_id)

    # 取消标记&计算
    elif action_flag == 'CANCEL_BOTH':
        # 将HHR_CAL_GRP_RUN状态置为2-新建
        update_cal_grp_run(db, tenant_id, cal_grp_id, action_user, action_flag)

        # 删除标记/计算结果、标记/计算消息表中当前日历对应的记录
        del_payee_calc_stat(db, tenant_id, cal_grp_id, action_user, 'ALL')

        # 删除人员薪资计算结果（多个表）
        del_py_calc_rslt(db, tenant_id, cal_grp_id, action_user)

        # 删除人员薪资计算过程日志
        del_py_calc_log(db, tenant_id, cal_grp_id)

        # 将人员追溯触发器表中“处理日历组”为当前日历组的“触发器状态”还原为U（未处理），并清空处理日历组
        u = text(
            "update boogoo_payroll.hhr_py_rto_trgr set hhr_trgr_status = 'U', hhr_pycalgrp_id = '' where tenant_id = :b1 and hhr_pycalgrp_id = :b2 ")
        db.conn.execute(u, b1=tenant_id, b2=cal_grp_id)

    # 完成计算
    elif action_flag == 'FINISH':
        # 将HHR_CAL_GRP_RUN状态置为5-已计算
        update_cal_grp_run(db, tenant_id, cal_grp_id, action_user, action_flag)

    # 关闭计算
    elif action_flag == 'CLOSE':
        # 将HHR_CAL_GRP_RUN状态置为8-已关闭
        update_cal_grp_run(db, tenant_id, cal_grp_id, action_user, action_flag)

        # 释放人员：将标记/计算结果表中的标记记录对应的锁定标识置为0
        sql_stat = text(
            "update boogoo_payroll.hhr_py_payee_calc_stat set hhr_lock_flg = '0' where tenant_id = :b1 and hhr_pycalgrp_id = :b2 ")
        db.conn.execute(sql_stat, b1=tenant_id, b2=cal_grp_id)


def get_upd_cal_id(db, tenant_id, cal_grp_id, cal_id, emp_id, emp_rcd):
    """获取人员的更正日历"""

    upd_cal_id = None
    t = text(
        "select hhr_upt_calid from boogoo_payroll.hhr_py_payee_calc_stat where tenant_id = :b1 and hhr_pycalgrp_id = :b2 and hhr_py_cal_id = :b3 "
        "and hhr_empid = :b4 and hhr_emp_rcd = :b5 ")
    row = db.conn.execute(t, b1=tenant_id, b2=cal_grp_id, b3=cal_id, b4=emp_id, b5=emp_rcd).fetchone()
    if row is not None:
        upd_cal_id = row['hhr_upt_calid']
    return upd_cal_id


def update_retro_trigger_by_emp(db, tenant_id, cal_grp_id, in_emp_id, in_emp_rcd):
    u = text("update boogoo_payroll.hhr_py_rto_trgr set hhr_trgr_status = 'U', hhr_pycalgrp_id = '' where tenant_id = :b1 "
             "and hhr_pycalgrp_id = :b2 and hhr_empid = :b3 and hhr_emp_rcd = :b4 ")
    db.conn.execute(u, b1=tenant_id, b2=cal_grp_id, b3=in_emp_id, b4=in_emp_rcd)


def get_emp_type(emp):
    """获取人员类型"""

    db = gv.get_db()
    prd_end_dt = gv.get_var_value("VR_PERIOD_END")
    t = text("select j.hhr_org_per_jobdata_attr01 from boogoo_corehr.hhr_org_per_jobdata j where j.tenant_id = :b1 "
             "and j.hhr_empid = :b2 and j.hhr_emp_rcd = :b3 and :b4 BETWEEN j.hhr_efft_date and j.hhr_efft_end_date ")
    row = db.conn.execute(t, b1=emp.tenant_id, b2=emp.emp_id, b3=emp.emp_rcd, b4=prd_end_dt).fetchone()
    if row is not None:
        return row['hhr_org_per_jobdata_attr01']
    else:
        return None


def get_main_cost_day(tenant_id):
    """获取薪资过账-主成本日期"""

    db = gv.get_db()
    stmt = text(
        "SELECT b.`value` As main_cost_day FROM hzero_platform.hpfm_profile a JOIN hzero_platform.hpfm_profile_value b ON b.profile_id = a.profile_id and b.level_code = 'GLOBAL' "
        "and b.level_value = 'GLOBAL' WHERE a.tenant_id = :b1 AND a.profile_name = 'HHR.PY.POST.MAIN_COST_DATE'")
    row = db.conn.execute(stmt, b1=tenant_id).fetchone()
    if row is not None:
        main_cost_day = int(row['main_cost_day'])
    else:
        raise Exception("未获取到薪资过账-主成本日期")
    return main_cost_day


def get_paygrp_by_cal_id(db, tenant_id, cal_id):
    stmt = text("select hhr_pygroup_id from boogoo_payroll.hhr_py_calendar where tenant_id = :b1 and hhr_py_cal_id = :b2 ")
    row = db.conn.execute(stmt, b1=tenant_id, b2=cal_id).fetchone()
    if row is not None:
        return row['hhr_pygroup_id']
    else:
        return None