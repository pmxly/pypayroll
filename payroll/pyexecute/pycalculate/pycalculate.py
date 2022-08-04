# coding:utf-8

from ....payroll.pysysutils.init_sys_variables import InitVariables
from ...pycalendar.calender import get_cal_obj_in_dev
from ...pysysutils import global_variables as gv
from .retro.retro_periods import RetroPeriods
from .calculate_by_emp import do_calculate, calc_new_entry_with_no_retro
from .table.catalog import init_catalog
from sqlalchemy.sql import text
from ...pysysutils.func_lib_01 import (update_py_calc_stat, ins_py_calc_msg, get_upd_cal_id, del_py_calc_rslt_by_emp,
                                       del_py_calc_log_by_emp, update_retro_trigger_by_emp)
from ...pyexecute.parameterentity import EmpParameter
import traceback
import time
import sys
from importlib import reload


def is_retro(cal_obj):
    """
    判断日历组和薪资组是否考虑追溯
    :return:True:追溯，False：不追溯
    """
    retro_cal_lst = gv.get_run_var_value('RETRO_CAL_LST')
    if cal_obj.cal_id in retro_cal_lst:
        gv.set_run_var_value('RETRO_FLAG', 'Y')
        return True
    else:
        gv.set_run_var_value('RETRO_FLAG', 'N')
        return False


def calculate_by_emp(emp):
    """
    按员工计算薪资
    :param emp:参数对象参数对象hhr.payroll.pyexecute.parameterentity.EmpParameter
    :return:
    """

    db = gv.get_db()
    cal_grp_obj = gv.get_run_var_value('CAL_GRP_OBJ')
    cal_obj = gv.get_run_var_value('CAL_OBJ')

    # 追溯标志，Y：做过追溯, N:没处理过追溯
    do_retro_flag = 'N'

    gv.set_run_var_value('RETRO_MIN', None)
    gv.set_run_var_value('RETRO_ACT', None)
    gv.set_run_var_value('RETRO_CATALOG_LIST', None)

    gv.set_run_var_value('UPD_CAL_ID', None)

    # 按员工设置过程日志树下一节点编号
    gv.set_run_var_value('LOG_TREE_NEXT_NUM', 20000)

    # gv.set_run_var_value('POSN_ASSOC', {})

    # 确定已有薪资计算记录的最大序号 Added by David on 2019/08/05
    s = text(
        "select max(hhr_seq_num) from boogoo_payroll.hhr_py_cal_catalog where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 ")
    max_row = db.conn.execute(s, b1=emp.tenant_id, b2=emp.emp_id, b3=emp.emp_rcd).fetchone()
    if max_row[0] is not None:
        max_catalog_seq = max_row[0]
    else:
        max_catalog_seq = None

    # 当周期运行&追溯
    if is_retro(cal_obj):
        # 计算追溯日历
        rto_catalogs = RetroPeriods(emp=emp, cal_obj=cal_obj, max_seq=max_catalog_seq)

        for retro_catalog in rto_catalogs.retro_periods:
            # 执行计算
            do_retro_flag = 'Y'

            """新入职的员工，被追溯的期间日历组ID和期间日历ID都为空，为便于计算，初始化为当前日历ID， 
            但后续属性不能从该日历对象中获取"""
            if not retro_catalog.f_calgrp_id:
                retro_catalog.f_calgrp_id = cal_grp_obj.cal_group_id
            if not retro_catalog.f_cal_id:
                retro_catalog.f_cal_id = cal_obj.cal_id
                retro_catalog.new_entry_flg = 'Y'

            retro_cal = get_cal_obj_in_dev(cal_obj.tenant_id, retro_catalog.f_cal_id)
            gv.set_run_var_value('RETRO_CATALOG_LIST', rto_catalogs.retro_periods)
            gv.set_run_var_value('CUR_CAL_OBJ', retro_cal)
            gv.set_run_var_value('PY_CATALOG', retro_catalog)

            # 被追溯的期间如果是更正类型，记录被更正的日历ID
            if retro_catalog.f_cal_type == 'C':
                tenant_id = retro_catalog.tenant_id
                f_cal_id = retro_catalog.f_cal_id
                emp_id = emp.emp_id
                emp_rcd = emp.emp_rcd
                upd_cal_id = get_upd_cal_id(db, tenant_id, f_cal_id, f_cal_id, emp_id, emp_rcd)
                gv.set_run_var_value('UPD_CAL_ID', upd_cal_id)
            else:
                gv.set_run_var_value('UPD_CAL_ID', None)

            do_calculate(emp, retro_cal, 'R')

    # 当周期运行&不追溯
    else:
        if cal_obj.run_type_entity.cycle == 'C':
            # 根据人员编号、任职记录号获取“触发器状态”=U（未处理）或者“处理日历组”=当前日历组的记录
            # 将“人员追溯触发器”表中的记录标记为“C-取消”，“处理日历组”更新为当前日历组
            u = text(
                "update boogoo_payroll.hhr_py_rto_trgr set hhr_trgr_status = 'C', hhr_pycalgrp_id = :b5 where (hhr_trgr_status = 'U' or hhr_pycalgrp_id = :b5) "
                "and tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 and hhr_country = :b4 ")
            db.conn.execute(u, b1=emp.tenant_id, b2=emp.emp_id, b3=emp.emp_rcd, b4=cal_grp_obj.country,
                            b5=cal_grp_obj.cal_group_id)

        """不追溯时针对新入职员工的特殊处理(例如员工A在1.23日入职，但是有些公司规定23号之后入职的须在下月算薪) Added by David on 2019/08/05"""

        if max_catalog_seq is None:  # 薪资计算目录中尚无记录
            if cal_obj.pay_cal_type in ['A', 'B'] and cal_obj.run_type_entity.cycle == 'C':
                retro_periods = calc_new_entry_with_no_retro(emp, cal_grp_obj, cal_obj)
                for retro_catalog in retro_periods:
                    """新入职的员工，被追溯的期间日历组ID和期间日历ID都为空，为便于计算，初始化为当前日历ID， 
                    但后续属性不能从该日历对象中获取"""
                    if not retro_catalog.f_calgrp_id:
                        retro_catalog.f_calgrp_id = cal_grp_obj.cal_group_id
                    if not retro_catalog.f_cal_id:
                        retro_catalog.f_cal_id = cal_obj.cal_id
                        retro_catalog.new_entry_flg = 'Y'

                    retro_cal = get_cal_obj_in_dev(cal_obj.tenant_id, retro_catalog.f_cal_id)
                    gv.set_run_var_value('RETRO_CATALOG_LIST', retro_periods)
                    gv.set_run_var_value('CUR_CAL_OBJ', retro_cal)
                    gv.set_run_var_value('PY_CATALOG', retro_catalog)
                    retro_cal = get_cal_obj_in_dev(cal_obj.tenant_id, retro_catalog.f_cal_id)
                    do_calculate(emp, retro_cal, 'R')

    # 计算更正日历
    if cal_obj.pay_cal_type == 'C':
        # # 获取历经期日历对象
        # upt_cal = get_cal_obj_in_dev(emp.tenant_id, emp.upd_cal_id)
        # gv.set_run_var_value('CUR_CAL_OBJ', upt_cal)
        # # 设置历经期日历对象
        # upt_catalog = init_catalog(emp, upt_cal, cal_obj, do_retro_flag)
        # gv.set_run_var_value('PY_CATALOG', upt_catalog)
        # # 执行计算
        # do_calculate(emp, upt_cal, 'C')

        # 获取历经期日历对象
        upt_cal = get_cal_obj_in_dev(emp.tenant_id, emp.upd_cal_id)
        # 历经期仍设置为当前日历
        gv.set_run_var_value('CUR_CAL_OBJ', cal_obj)
        # 记录被更正日历ID
        gv.set_run_var_value('UPD_CAL_ID', emp.upd_cal_id)
        # 设置历经期日历对象
        upt_catalog = init_catalog(emp, upt_cal, cal_obj, do_retro_flag)
        gv.set_run_var_value('PY_CATALOG', upt_catalog)
        # 执行计算
        do_calculate(emp, cal_obj, 'C')

    # 计算当前日历
    if cal_obj.pay_cal_type in ['A', 'B']:
        gv.set_run_var_value('CUR_CAL_OBJ', cal_obj)
        # 设置历经期目录对象
        catalog = init_catalog(emp, cal_obj, cal_obj, do_retro_flag)
        gv.set_run_var_value('PY_CATALOG', catalog)
        gv.set_run_var_value('UPD_CAL_ID', None)
        # 执行计算
        do_calculate(emp, cal_obj, 'N')


# def check_payees_calc_status(tenant_id, cal_grp_id):
#     """判断是否所有人员都已经成功计算，更新日历/日历组运行状态"""
#
#     db = gv.get_db()
#     sql = text("select 'Y' from HHR_PAYEE_CALC_STAT where tenant_id = :b1 and HHR_PYCALGRP_ID = :b2 and HHR_PAYEE_CALC_STAT <> 'S' ")
#     r = db.conn.execute(sql, b1=tenant_id, b2=cal_grp_id).fetchone()
#     if r is None:
#         return True
#     else:
#         return False


def calculate_by_emp_list(run_parameter):
    """
    按人员list计算薪资
    :param run_parameter:参数字典
    :return:
    """

    db = gv.get_db()
    init_var = InitVariables()

    gv.set_run_var_value('RUN_PARM', run_parameter)
    # 初始化日历组相关变量
    cur_calendar_group = gv.get_run_var_value('CAL_GRP_OBJ')
    init_var.init_cal_grp_variable(cur_calendar_group)

    cal_grp_id = run_parameter['cal_grp_id']
    user = run_parameter['operator_user_id']
    # py_calculate_flag = run_parameter['py_calculate_flag']
    # re_calc_flag = run_parameter['py_calculate_flag']
    # re_iden_calc_flag = run_parameter['re_iden_calc_flag']
    # print('------run_parameter[payees]-------' + str(run_parameter['payees']))
    comm_log_flag = gv.get_run_var_value('COMM_LOG_FLAG')

    gv.set_run_var_value('POSN_ASSOC', {})
    gv.set_run_var_value('ATT_TYPE_DATA', {})
    gv.set_run_var_value('ABS_PTCODE_DATA', {})

    for emp_dic in run_parameter['payees']:
        # start = time.time()

        tenant_id = emp_dic['tenant_id']
        emp_id = emp_dic['emp_id']
        emp_rcd = emp_dic['emp_rcd']
        emp_log_flag = emp_dic['emp_log_flag']

        # trans = db.conn.begin()
        del_py_calc_rslt_by_emp(db, tenant_id, cal_grp_id, user, emp_id, emp_rcd)
        del_py_calc_log_by_emp(db, tenant_id, cal_grp_id, emp_id, emp_rcd)
        update_retro_trigger_by_emp(db, tenant_id, cal_grp_id, emp_id, emp_rcd)
        # trans.commit()

        if comm_log_flag == 'Y':
            pre_log_flag = gv.get_run_var_value('LOG_FLAG')
            gv.set_run_var_value('PRE_LOG_FLAG', pre_log_flag)
            if emp_log_flag == 'Y':
                gv.set_run_var_value('LOG_FLAG', 'Y')
            else:
                gv.set_run_var_value('LOG_FLAG', 'N')

            if pre_log_flag and pre_log_flag != gv.get_run_var_value('LOG_FLAG'):
                for k, m in sys.modules.items():
                    if 'hhr.payroll' in k:
                        reload(m)
        else:
            gv.set_run_var_value('LOG_FLAG', 'N')
            gv.set_run_var_value('PRE_LOG_FLAG', 'N')

        # 在日历池中获取日历对象
        calendar = get_cal_obj_in_dev(emp_dic['tenant_id'], emp_dic['cal_id'])
        # 将所在期日历对象添加到dev变量中
        gv.set_run_var_value('CAL_OBJ', calendar)
        # 初始化所在期日历相关变量
        init_var.init_cal_variable(calendar)

        # 计算单个员工
        # trans = db.conn.begin() 2021-02-02
        emp = EmpParameter()
        try:
            emp.tenant_id = tenant_id
            emp.cal_grp_id = emp_dic['cal_grp_id']
            emp.cal_id = emp_dic['cal_id']
            emp.upd_cal_id = emp_dic['upd_cal_id']
            emp.emp_id = emp_id
            emp.emp_rcd = emp_rcd

            calculate_by_emp(emp)

            # 更新人员计算状态
            val = {'db': db, 'tenant_id': tenant_id, 'cal_grp_id': cal_grp_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd,
                   'status': 'S', 'user': user}
            update_py_calc_stat(**val)

            # # 判断是否所有人员都已经成功计算，更新日历/日历组运行状态
            # if check_payees_calc_status(tenant_id, cal_grp_id):
            #     if re_iden_calc_flag == 'Y':
            #         update_cal_grp_run(db, tenant_id, cal_grp_id, user, 'RE_IDEN_CALC')
            #     elif re_calc_flag == 'Y':
            #         update_cal_grp_run(db, tenant_id, cal_grp_id, user, 'RE_CALC')
            #     elif py_calculate_flag == 'Y':
            #         update_cal_grp_run(db, tenant_id, cal_grp_id, user, 'PY_CALC')

            # trans.commit() 2021-02-02
            db.conn.close()
        except Exception as e:
            # trans.rollback() 2021-02-02

            catalog = gv.get_run_var_value('PY_CATALOG')
            if catalog is not None:
                f_cal_id = catalog.f_cal_id
            else:
                f_cal_id = ''
            # ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_grp_id, f_cal_id=f_cal_id, emp_id=emp_id,
            #                 emp_rcd=emp_rcd, msg_class='B', msg_type='E', msg_txt=str(e), user_id=user)

            # ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_grp_id, f_cal_id=f_cal_id, emp_id=emp_id,
            #                 emp_rcd=emp_rcd, msg_class='B', msg_type='E', msg_txt=traceback.format_exc(limit=None), user_id=user)
            print(traceback.format_exc(limit=None))
            ins_py_calc_msg(tenant_id=tenant_id, cal_grp_id=cal_grp_id, cal_id=cal_grp_id, f_cal_id=f_cal_id,
                            emp_id=emp_id,
                            emp_rcd=emp_rcd, msg_class='B', msg_type='E', msg_txt=repr(e), user_id=user)
            update_py_calc_stat(**{'db': db, 'tenant_id': tenant_id, 'cal_grp_id': cal_grp_id,
                                   'emp_id': emp_id, 'emp_rcd': emp_rcd, 'status': 'E', 'user': user})
            db.conn.close()

        # end = time.time()
        # print("[" + emp.emp_id + "]" + '---Time elapsed: ' + str(end - start))
