# coding:utf-8
"""
按员工计算
"""
from ...pysysutils import global_variables as gv
from ...pysysutils.init_sys_variables import InitVariables
from .table.accm_cal import init_acc_cal
from .table.exrate import init_ex_rate
from ...pypins.pin_util import persist_pins_data
from ...pyvariables.var_util.varutil import persist_vars_data
from sqlalchemy.sql import text


def do_calculate(emp, cur_cal, calculate_type):
    """
    执行计算
    :param emp:参数对象参数对象hhr.payroll.pyexecute.parameterentity.EmpParameter
    :param cur_cal:历经期日历对象
    :param calculate_type:计算类型 R:追溯 C:更正 N：正常计算
    :return:
    """

    from ...pyruntype.run_type_util import exec_run_type

    catalog = gv.get_run_var_value('PY_CATALOG')

    init_var = InitVariables()

    # 初始化所有按期间初始化的变量
    init_var.init_all_vars_by_period(catalog)

    # 初始化人员基本信息变量
    init_var.init_emp_variable(emp)

    # 按人员期间初始化变量
    init_var.init_var_by_period()

    # 初始化分段信息，并放进全局变量中
    # gv.set_run_var_value('SEG_INFO_OBJ', SegInfo())

    # 设置计算类型
    gv.set_run_var_value('CUR_CALCULATE_TYPE', calculate_type)
    # 设置历经期日历对象
    # gv.set_run_var_value('CUR_CAL_OBJ', cur_cal)

    # 初始化历经期日历对象相关变量
    init_var.init_cur_cal_variable(catalog)
    # 获取累计日历
    init_acc_cal(catalog=catalog)
    # 设置期间状态
    gv.set_var_value('VR_PERIOD_STATUS', calculate_type)
    # 初始化历经期薪资组对象相关变量
    init_var.init_cur_py_group_var(cur_cal)
    # 初始化任职分段相关变量
    init_var.init_job_seg_var()

    # 按历经期获取汇率数据
    init_ex_rate()

    # 根据历经期日历，适用资格组，通用薪资项目初始化薪资项目
    # init_emp_pin_dic(catalog)

    # 根据历经期运行类型获取运程过程执行计算
    exec_run_type(emp.tenant_id, catalog.f_runtype_id)

    # 写入薪资项目
    persist_pins_data()

    # 写入变量
    persist_vars_data()


def calc_without_catalog(db, actual_rto_date, emp, cal_grp_obj, cal_obj):
    from ...pysysutils.func_lib_01 import payee_pay_group_check
    from .table.catalog import Catalog

    retro_periods = []
    tenant_id = emp.tenant_id
    emp_id = emp.emp_id
    emp_rcd = emp.emp_rcd

    cal_grp_id = cal_grp_obj.cal_group_id
    cal_id = cal_obj.cal_id
    country = cal_grp_obj.country
    pygrp_id = cal_obj.py_group_id
    run_type_id = cal_obj.run_type_id
    period_cd = cal_obj.period_id
    period_year = cal_obj.period_year
    period_num = cal_obj.period_num
    pay_date = cal_obj.pay_date
    cal_type = cal_obj.pay_cal_type
    prd_bgn_dt = cal_obj.period_cal_entity.start_date
    prd_end_dt = cal_obj.period_cal_entity.end_date
    cycle = cal_obj.run_type_entity.cycle

    if actual_rto_date < cal_obj.period_cal_entity.start_date:
        s = text("select hhr_period_year, hhr_prd_num, hhr_period_start_date, hhr_period_end_date "
                 "from boogoo_payroll.hhr_py_period_calendar_line where tenant_id = :b1 "
                 "and hhr_period_code = :b2 and hhr_period_end_date >= :b3 and hhr_period_end_date < :b4 order by hhr_period_end_date ")
        result = db.conn.execute(s, b1=tenant_id, b2=cal_obj.period_cal_entity.period_id, b3=actual_rto_date, b4=cal_obj.period_cal_entity.end_date).fetchall()
        seq_num = 0
        if result is None:
            return retro_periods
        for row in result:
            temp_pre_seq = seq_num
            seq_num += 1
            f_period_year = row['hhr_period_year']
            f_prd_num = row['hhr_prd_num']
            f_prd_bgn_dt = row['hhr_period_start_date']
            f_prd_end_dt = row['hhr_period_end_date']

            # 若在本次薪资计算的开始日期前，薪资组不同，则不需要追溯
            if payee_pay_group_check(emp, cal_obj, bgn_dt=f_prd_bgn_dt, end_dt=f_prd_end_dt):
                if seq_num == 1:
                    prev_seq = None
                else:
                    prev_seq = temp_pre_seq
                row_dict = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num,
                            'f_calgrp_id': '', 'f_cal_id': '', 'f_country': country, 'f_pygrp_id': pygrp_id,
                            'f_runtype_id': run_type_id,
                            'f_period_cd': period_cd, 'f_period_year': f_period_year,
                            'f_prd_num': f_prd_num, 'f_pay_date': pay_date,
                            'f_cal_type': cal_type, 'f_prd_bgn_dt': f_prd_bgn_dt,
                            'f_prd_end_dt': f_prd_end_dt, 'f_rt_cycle': cycle,
                            'cal_grp_id': cal_grp_id, 'cal_id': cal_id, 'country': country,
                            'pygrp_id': pygrp_id, 'run_type_id': run_type_id,
                            'period_cd': period_cd, 'period_year': period_year, 'period_num': period_num,
                            'pay_date': pay_date,
                            'cal_type': cal_type, 'prd_bgn_dt': prd_bgn_dt, 'prd_end_dt': prd_end_dt,
                            'cycle': cycle, 'rec_stat': 'A',
                            'prev_seq': prev_seq, 'hist_seq': None}
                catalog = Catalog(**row_dict)
                catalog.insert()
                retro_periods.append(catalog)
    return retro_periods


def calc_new_entry_with_no_retro(emp, cal_grp_obj, cal_obj):
    from ...pysysutils.func_lib_01 import get_emp_type

    retro_periods = []
    db = gv.get_db()
    vr_autocal_entry = gv.get_var_value('VR_AUTOCAL_ENTRY')
    vr_autocal_period = gv.get_var_value('VR_AUTOCAL_PERIOD')

    if vr_autocal_entry == 'Y' and vr_autocal_period >= 1:
        emp_type = get_emp_type(emp)
        # 正式工（人员类别：1、2）需要自动追溯，非正式工不能自动追溯
        if emp_type and emp_type in ['1', '2']:
            # 获取最小追溯日期（新员工自动追溯）
            min_retro_ne_date = cal_obj.min_rto_prd_cal_for_new_entry.start_date
            # 最小追溯日期（新员工自动追溯）
            t1 = text("select min(a.hhr_efft_date) from boogoo_payroll.hhr_py_assign_pg a where a.tenant_id = :b1 "
                      "and a.hhr_empid = :b2 and hhr_emp_rcd = :b3 ")
            r = db.conn.execute(t1, b1=emp.tenant_id, b2=emp.emp_id, b3=emp.emp_rcd, b4=min_retro_ne_date).fetchone()
            if r[0] is not None:
                min_eff_date = r[0]
                if min_eff_date > min_retro_ne_date:
                    auto_cal_act_date = min_eff_date
                else:
                    auto_cal_act_date = min_retro_ne_date
                retro_periods = calc_without_catalog(db, auto_cal_act_date, emp, cal_grp_obj, cal_obj)
    return retro_periods

