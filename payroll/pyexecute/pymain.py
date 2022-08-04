# coding:utf-8

"""
薪酬计算入口模块
create by wangling 2018/8/7
"""

from ..pysysutils import global_variables as gv
from ..pycalendar.calender import CalendarGroup
from .identifyemployees.identify_employees import IdentifyEmployees
from ..logger.logger import Logger
from .pycalculate.pycalculate import calculate_by_emp_list
from ..pysysutils.func_lib_01 import get_run_payees, chunk_payees
from ..pysysutils.func_lib_01 import ins_py_cal_task
from ...confreader import conf
from ...payroll.pysysutils.init_sys_variables import InitVariables
from ...payroll.pysysutils.func_lib_01 import del_payee_calc_stat, del_py_calc_rslt, del_py_calc_log, get_main_cost_day
from sqlalchemy.sql import text
from datetime import datetime, timedelta
from ..pysysutils.switch import get_switch_values


def run_dist_py_engine(run_param):
    """
    Desc: 分布式薪资计算入口函数
    Author: David
    Date: 2018/08/13
    :param run_param：参数对象pyexecute.parameterentity.RunParameter
    :return:
    """
    gv.init(run_param['task_db'])
    run_param.pop('task_db')
    tenant_id = run_param['tenant_id']

    # 初始化日历组对象
    calendar_group_obj = CalendarGroup(tenant_id, run_param['cal_grp_id'])
    # 将日历组对象放进DEV变量
    gv.set_run_var_obj('CAL_GRP_OBJ', '日历组对象', calendar_group_obj)
    gv.set_run_var_value('CAL_GRP_OBJ', calendar_group_obj)

    init_var = InitVariables()
    init_var.init_all_variables(tenant_id)

    gv.set_run_var_value('TENANT_ID', tenant_id)
    gv.set_run_var_value('MAIN_COST_DAY', get_main_cost_day(tenant_id))

    # 获取控制开关值 Added by David on 2019/08/05
    sw_val_dic = get_switch_values(tenant_id, ['PY_AUTOCAL_ENTRY', 'PY_AUTOCAL_PERIOD'])
    py_autocal_entry = sw_val_dic.get('PY_AUTOCAL_ENTRY', '')
    py_autocal_period = sw_val_dic.get('PY_AUTOCAL_PERIOD', '')
    if py_autocal_period.isdigit():
        py_autocal_period = int(py_autocal_period)
    else:
        py_autocal_period = 0
    gv.set_var_value('VR_AUTOCAL_ENTRY', py_autocal_entry)
    gv.set_var_value('VR_AUTOCAL_PERIOD', py_autocal_period)

    gv.set_run_var_value('IDENTIFY_EMP_FLAG', run_param['identify_emp_flag'])
    # gv.set_run_var_value('PY_CALCULATE_FAG', run_param.py_calculate_flag)
    # gv.set_run_var_value('CANCEL_CALCULATE_FLAG', run_param.cancel_calculate_flag)
    # gv.set_run_var_value('COMPLETE_FLAG', run_param.complete_flag)
    gv.set_run_var_value('COMM_LOG_FLAG', run_param['log_flag'])
    gv.set_run_var_value('TASK_ID', run_param['task_id'])

    tenant_id = run_param['tenant_id']
    task_id = run_param['task_id']
    task_name = run_param['task_name']
    logger = Logger(tenant_id, task_id)
    gv.set_run_var_value('LOGGER', logger)

    # 将日历组下的所有日历放进DEV变量
    gv.set_run_var_value('CAL_OBJ_DIC', calendar_group_obj.calendar_dic)

    if task_name == 'pyidentify':
        identify_employees(run_param)

    elif task_name == 'pycalc':
        calculate_by_emp_list(run_param)


def schedule_py_cal_prcs(run_param):
    """
    Desc: 调用分布式进程
    Author: David
    Date: 2018/08/21
    """

    from ...taskspool.core.pycalc import pycalc
    run_payees = get_run_payees(run_param)
    # payees_log_lmt = conf.getint('config', 'payroll_payees_log_lmt')
    threshold = conf.getint('config', 'payroll_payees_threshold')

    # if len(run_payees) > payees_log_lmt:
    #     run_param['log_flag'] = 'N'
    pay_group_id = run_param['pay_group_id']
    if pay_group_id in ['CA', 'CM', 'CC']:
        queue = 'DEDICATED_Q'
    else:
        queue = 'SHARE_Q'

    if len(run_payees) >= threshold:
        prcs_num = conf.getint('config', 'payroll_parallel_num')
        if prcs_num > 0:
            payees_per_prcs = int(len(run_payees) / prcs_num) + 1
            chunk_lst = list(chunk_payees(run_payees, payees_per_prcs))
            for item in chunk_lst:
                kw = dict()
                run_param['payees'] = item
                run_param['task_name'] = 'pycalc'
                kw['tenant_id'] = run_param['tenant_id']
                kw['user_id'] = run_param['operator_user_id']
                kw['run_ctl'] = run_param['run_ctl_id']
                kw['run_param'] = run_param
                result = pycalc.apply_async(kwargs=kw, queue=queue, retry=False)
            return len(chunk_lst)
    elif len(run_payees) > 0:
        kw = dict()
        run_param['payees'] = run_payees
        run_param['task_name'] = 'pycalc'
        kw['tenant_id'] = run_param['tenant_id']
        kw['user_id'] = run_param['operator_user_id']
        kw['run_ctl'] = run_param['run_ctl_id']
        kw['run_param'] = run_param
        result = pycalc.apply_async(kwargs=kw, queue=queue, retry=False)
        return 1


def identify_employees(run_parameter):
    db = gv.get_db()
    tenant_id = run_parameter['tenant_id']
    cal_grp_id = run_parameter['cal_grp_id']
    user_id = run_parameter['operator_user_id']

    # trans = db.conn.begin() 2021-03-11
    try:
        ie = IdentifyEmployees(run_parameter)
        ie.identify_employees()
        if run_parameter['py_calculate_flag'] == 'Y' or run_parameter['re_calc_flag'] == 'Y':
            run_ctl_id = str(tenant_id) + '~' + cal_grp_id + '~' + (
                (datetime.now() + timedelta(seconds=1)).strftime('%Y%m%d%H%M%S'))
            run_parameter['run_ctl_id'] = run_ctl_id

            if run_parameter['re_calc_flag'] == 'Y':
                # t1 = db.conn.begin() 2021-03-11
                # 删除标记/计算结果、标记/计算消息表中当前日历对应的记录（仅薪资计算记录）
                del_payee_calc_stat(db, tenant_id, cal_grp_id, user_id, 'PAY', run_ctl_id=run_ctl_id)
                # t1.commit() 2021-03-11

                # t2 = db.conn.begin() 2021-03-11
                # 删除人员薪资计算结果（多个表）/ 更新薪资计算目录
                del_py_calc_rslt(db, tenant_id, cal_grp_id, user_id)
                # t2.commit() 2021-03-11

                # t3 = db.conn.begin() 2021-03-11
                # 删除人员薪资计算过程日志
                del_py_calc_log(db, tenant_id, cal_grp_id)
                # t3.commit() 2021-03-11

                # 将人员追溯触发器表中“处理日历组”为当前日历组的“触发器状态”还原为U（未处理），并清空处理日历组
                u = text(
                    "update boogoo_payroll.hhr_py_rto_trgr set hhr_trgr_status = 'U', hhr_pycalgrp_id = '' where tenant_id = :b1 and hhr_pycalgrp_id = :b2 ")
                db.conn.execute(u, b1=tenant_id, b2=cal_grp_id)
            prcs_num = schedule_py_cal_prcs(run_parameter)
            if prcs_num:
                ins_py_cal_task(db, tenant_id, cal_grp_id, cal_grp_id, run_ctl_id, 'pycalc', user_id, prcs_num)

        # trans.commit() 2021-03-11
        db.conn.close()
    except Exception as e:
        # trans.rollback() 2021-03-11
        db.conn.close()
        raise e
