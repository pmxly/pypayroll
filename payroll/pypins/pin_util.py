# coding:utf-8
"""
薪资项目工具方法
create by wangling 2018/9/13
"""
from ..pysysutils import global_variables as gv
from sqlalchemy import text
from copy import deepcopy
from ..pyexecute.pycalculate.table.pin_seg import PinSeg


def get_cal_pin_dic(catalog):
    """
    create by wangling 2018/9/13
    获取一套日历对应的薪资项目字典
    :return:包含日历ID对应的薪资项目
    """

    from ..pypins.pin import Pin

    tenant_id = catalog.tenant_id
    f_end_dt = catalog.f_prd_end_dt

    calendar_all_dic = gv.get_run_var_value('CAL_ALL_PIN_DIC')
    if calendar_all_dic is None:
        calendar_all_dic = dict()
    key = str(tenant_id) + '_' + str(f_end_dt)
    if key in calendar_all_dic:
        return calendar_all_dic[key]
    else:
        pin_dic = {}
        db = gv.get_db()
        sql = text(
            "select distinct a.hhr_pin_cd from boogoo_payroll.hhr_py_pin a where a.tenant_id = :b1 and a.hhr_status = 'Y' "
            "and :b2 between a.hhr_efft_date and a.hhr_efft_end_date ")
        result = db.conn.execute(sql, b1=tenant_id, b2=f_end_dt).fetchall()
        if result is not None:
            for result_line in result:
                pin_dic[result_line['hhr_pin_cd']] = Pin(tenant_id, result_line['hhr_pin_cd'], f_end_dt)
            calendar_all_dic[key] = pin_dic
            gv.set_run_var_value('CAL_ALL_PIN_DIC', calendar_all_dic)
            return pin_dic


def init_general_pins(catalog):
    """
    Desc: 根据历经期国家/地区，从通用薪资项目定义表中获取历经期内允许的薪资项目
    Author: David
    Date: 2018/09/10
    """

    tenant_id = catalog.tenant_id
    f_bgn_date = catalog.f_prd_bgn_dt
    f_end_date = catalog.f_prd_end_dt
    country = catalog.f_country

    dic_key = country + str(f_bgn_date) + str(f_end_date)
    if dic_key is None or country is None:
        return
    gen_pins_dic = gv.get_run_var_value('GENERAL_PINS_DIC')
    if gen_pins_dic is None:
        gen_pins_dic = dict()
        gen_pins_dic[dic_key] = {}
    else:
        if dic_key not in gen_pins_dic:
            # gen_pins_dic = { '2018_01P': {'P1': P1_OBJ, 'P2': P2_OBJ, } }
            gen_pins_dic[dic_key] = {}
    if len(gen_pins_dic[dic_key]) == 0:
        db = gv.get_db()
        txt = text("select a.hhr_pin_cd from boogoo_payroll.hhr_py_general_pin a where "
                   "a.tenant_id = :b1 and a.hhr_country = :b2 "
                   "and ( (:b3 <= a.hhr_end_dt and :b4 >= a.hhr_start_dt) or (a.hhr_end_dt is null and :b4 >=a.hhr_start_dt) ) ")
        rs = db.conn.execute(txt, b1=tenant_id, b2=country, b3=f_bgn_date, b4=f_end_date).fetchall()
        for row in rs:
            pin_cd = row['hhr_pin_cd']
            gen_pins_dic[dic_key][pin_cd] = deepcopy(get_cal_pin_dic(catalog)[pin_cd])

        gv.set_run_var_value('GENERAL_PINS_DIC', gen_pins_dic)


def init_scope_pins(catalog):
    """
    Desc: 根据适用范围从适用范围的薪资项目表中获取历经期内允许的薪资项目
    Author: David
    Date: 2018/09/10
    """

    tenant_id = catalog.tenant_id
    f_bgn_date = catalog.f_prd_bgn_dt
    f_end_date = catalog.f_prd_end_dt
    scope = gv.get_var_value('VR_SCOPE')
    if scope is None:
        return
    dic_key = scope + str(f_bgn_date) + str(f_end_date)
    pins_scope_dic = gv.get_run_var_value('SCOPE_PINS_DIC')
    if pins_scope_dic is None:
        pins_scope_dic = dict()
        pins_scope_dic[dic_key] = {scope: {}}
    else:
        if dic_key not in pins_scope_dic:
            # pins_scope_dic = {'2018_01P': {'SCOPE1': { 'P1': P1_OBJ, 'P2': P2_OBJ }, 'SCOPE2': {......},} }
            pins_scope_dic[dic_key] = {scope: {}}
        elif scope not in pins_scope_dic[dic_key]:
            pins_scope_dic[dic_key] = {scope: {}}
    if len(pins_scope_dic[dic_key][scope]) == 0:
        db = gv.get_db()
        txt = text(
            "select a.hhr_pin_cd from boogoo_payroll.hhr_py_app_scope_line a where "
            "a.tenant_id = :b1 and a.hhr_app_scope_code = :b2 "
            "and ( (:b3 <= a.hhr_end_date and :b4 >= a.hhr_bgn_date) or (a.hhr_end_date is null and :b4 >=a.hhr_bgn_date) ) ")
        rs = db.conn.execute(txt, b1=tenant_id, b2=scope, b3=f_bgn_date, b4=f_end_date).fetchall()
        for row in rs:
            pin_cd = row['hhr_pin_cd']
            pins_scope_dic[dic_key][scope][pin_cd] = deepcopy(get_cal_pin_dic(catalog)[pin_cd])
    gv.set_run_var_value('SCOPE_PINS_DIC', pins_scope_dic)


def init_agency_company(tenant_id, scope):
    db = gv.get_db()
    scope_agency_dic = gv.get_run_var_value('SCOPE_AGENCY_DIC')
    key = str(tenant_id) + "_" + scope
    if key not in scope_agency_dic:
        t = text("select hhr_company_code, hhr_agency_company_code from boogoo_payroll.hhr_py_app_scope_head "
                 "where tenant_id = :b1 and hhr_app_scope_code = :b2")
        row = db.conn.execute(t, b1=tenant_id, b2=scope).fetchone()
        if row is not None:
            company_cd = row['hhr_company_code']
            agency_company_cd = row['hhr_agency_company_code']
            gv.set_var_value('VR_SCOPE_COMPANY', company_cd)
            gv.set_var_value('VR_AGENT_COMPANY', agency_company_cd)
            scope_agency_dic[key] = (company_cd, agency_company_cd)
    else:
        agency_info = scope_agency_dic[key]
        gv.set_var_value('VR_SCOPE_COMPANY', agency_info[0])
        gv.set_var_value('VR_AGENT_COMPANY', agency_info[1])


def init_emp_pin_dic(catalog):
    """
    根据历经期日历，适用资格组，通用薪资项目初始化薪资项目
    :return:
    """
    f_bgn_date = catalog.f_prd_bgn_dt
    f_end_date = catalog.f_prd_end_dt
    country = catalog.f_country
    dic_key1 = country + str(f_bgn_date) + str(f_end_date)

    scope = gv.get_var_value('VR_SCOPE')
    init_agency_company(catalog.tenant_id, scope)
    if scope is None:
        scope = ''
    dic_key2 = scope + str(f_bgn_date) + str(f_end_date)

    # 根据历经期初始化所有薪资项目
    get_cal_pin_dic(catalog)
    # 按历经期获取通用系统薪资项目
    init_general_pins(catalog)
    # 按历经期获取适用范围薪资项目
    init_scope_pins(catalog)

    # 按历经期初始化薪资项目处理对象，供基础薪酬/经常性支付/一次性支付处理函数使用
    gv.set_run_var_value('PIN_PROCESS_OBJ', None)
    # 初始化考勤处理对象
    gv.set_run_var_value('PT_PROCESS_OBJ', None)

    # 初始化通用薪资项目
    seg_info = deepcopy(gv.get_run_var_value('SEG_INFO_OBJ'))
    temp_dic = gv.get_run_var_value('GENERAL_PINS_DIC')

    if temp_dic is not None:
        if dic_key1 in temp_dic:
            gen_pin_dic = temp_dic[dic_key1]
            for g_pin_obj in gen_pin_dic.values():
                tmp_g_pin_obj = deepcopy(g_pin_obj)
                tmp_g_pin_obj.create_segment(seg_info)
                gv.set_pin(tmp_g_pin_obj)

    # 初始化适用范围薪资项目
    temp_dic = gv.get_run_var_value('SCOPE_PINS_DIC')
    if temp_dic is not None:
        if dic_key2 in temp_dic:
            if dic_key2 in temp_dic:
                scope = gv.get_var_value('VR_SCOPE')
                scope_pin_dic = temp_dic[dic_key2][scope]
                for s_pin_obj in scope_pin_dic.values():
                    tmp_s_pin_obj = deepcopy(s_pin_obj)
                    tmp_s_pin_obj.create_segment(seg_info)
                    gv.set_pin(tmp_s_pin_obj)


def persist_pins_data():
    """将薪资项目写入到薪资项目结果表中"""

    catalog = gv.get_run_var_value('PY_CATALOG')
    tenant_id = catalog.tenant_id
    emp_id = catalog.emp_id
    emp_rcd = catalog.emp_rcd
    seq_num = catalog.seq_num
    pin_dic = gv.get_pin_dic()

    # trans = gv.get_db().conn.begin() 2021-02-02
    for pin_cd, pin_obj in pin_dic.items():
        if pin_obj.amt == 0 and pin_obj.std_amt == 0 and pin_obj.retro_amt == 0 and pin_obj.quantity == 0 and pin_obj.prcs_flag == '':
            continue
        segments = pin_obj.segment
        for seg in segments.values():
            v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'seq_num': seq_num, 'seg': seg}
            PinSeg(**v_dic).insert()
    # trans.commit() 2021-02-02
