# coding:utf-8

from decimal import *
from ..pysysutils import global_variables as gv
from sqlalchemy import text
from ..pyfunctions.function_object import ReturnObj


def round_rule(rule_id, value):
    """ create by wangling 2018/8/9
    R01 四舍五入(2位小数)
    R02 四舍五入(1位小数)
    R03 四舍五入(无小数)
    R04 向下舍入(2位小数)
    R05 向下舍入(1位小数)
    R06 向下舍入(无小数)
    R07 向下舍入(0.5进位)
    R08 向上舍入(2位小数)
    R09 向上舍入(1位小数)
    R10 向上舍入(无小数)
    R11 向上舍入(0.5进位)
    """

    if value is None:
        value = 0
    decimal_value = Decimal(str(abs(value)))
    # 四舍五入(2位小数)
    if rule_id == 'R01':
        # 解决2467.8250不能正确四舍五入的问题
        decimal_value = decimal_value + Decimal('0.000000000001')
        decimal_value = decimal_value.quantize(Decimal('0.00'), ROUND_HALF_EVEN)
    # 四舍五入(1位小数)
    elif rule_id == 'R02':
        decimal_value = decimal_value + Decimal('0.000000000001')
        decimal_value = decimal_value.quantize(Decimal('0.0'), ROUND_HALF_EVEN)
    # 四舍五入(无小数)
    elif rule_id == 'R03':
        decimal_value = decimal_value + Decimal('0.000000000001')
        decimal_value = decimal_value.quantize(Decimal('0'), ROUND_HALF_EVEN)
    # 向下舍入(2位小数)
    elif rule_id == 'R04':
        decimal_value = decimal_value.quantize(Decimal('0.00'), ROUND_FLOOR)
    # 向下舍入(1位小数)
    elif rule_id == 'R05':
        decimal_value = decimal_value.quantize(Decimal('0.0'), ROUND_FLOOR)
    # 向下舍入(无小数)
    elif rule_id == 'R06':
        decimal_value = decimal_value.quantize(Decimal('0'), ROUND_FLOOR)
    # 向下舍入(0.5进位)
    elif rule_id == 'R07':
        decimal05_value = decimal_value.quantize(Decimal('0'), ROUND_DOWN)
        decimal05_value = decimal_value + Decimal('0.5')
        if decimal05_value <= decimal_value:
            decimal_value = decimal05_value
        else:
            decimal_value = decimal_value.quantize(Decimal('0'), ROUND_DOWN)
    # 向上舍入(2位小数)
    elif rule_id == 'R08':
        decimal_value = decimal_value.quantize(Decimal('0.00'), ROUND_CEILING)
    # 向上舍入(1位小数)
    elif rule_id == 'R09':
        decimal_value = decimal_value.quantize(Decimal('0.0'), ROUND_CEILING)
    # 向上舍入(无小数)
    elif rule_id == 'R10':
        decimal_value = decimal_value.quantize(Decimal('0'), ROUND_CEILING)
    # 向上舍入(0.5进位)
    elif rule_id == 'R11':
        decimal05_value = decimal_value.quantize(Decimal('0'), ROUND_DOWN)
        decimal05_value = decimal_value + Decimal('0.5')
        if decimal05_value >= decimal_value:
            decimal_value = decimal05_value
        else:
            decimal_value = decimal_value.quantize(Decimal('0'), ROUND_DOWN) + 1

    if value < 0:
        return 0 - decimal_value
    else:
        return decimal_value


def get_acc_year_seq(add_type, period_cd=None, period_year=None, period_num=None):
    """
    获取累计年度和累计序号
    Created by David on 2018/09/18
    :param add_type: 累计类型
    :param period_cd: 期间编码
    :param period_year: 期间年度
    :param period_num: 期间序号
    :return:(累计年度,累计序号)
    """

    from ..pyexecute.pycalculate.table.accm_cal import AccumulateCalendar

    catalog = gv.get_run_var_value('PY_CATALOG')
    tenant_id = catalog.tenant_id
    if period_cd is None:
        period_cd = catalog.f_period_cd
    if period_year is None:
        period_year = catalog.f_period_year
    if period_num is None:
        period_num = catalog.f_prd_num

    acc_key = str(period_cd) + str(period_year) + str(period_num)
    acc_cal_dic = gv.get_run_var_value('ACC_CAL_DIC')
    if acc_key not in acc_cal_dic:
        acc_cal_dic[acc_key] = []
    if len(acc_cal_dic[acc_key]) == 0:
        db = gv.get_db()
        stmt = text("select hhr_period_add_type, hhr_period_add_year, hhr_period_add_number from boogoo_payroll.hhr_py_period_add_calendar_line "
                    "where tenant_id = :b1 and hhr_period_code = :b2 and hhr_period_year = :b3 and hhr_prd_num = :b4 ")
        rs = db.conn.execute(stmt, b1=tenant_id, b2=period_cd, b3=period_year, b4=period_num).fetchall()
        for row in rs:
            add_type = row['hhr_period_add_type']
            add_year = row['hhr_period_add_year']
            add_num = row['hhr_period_add_number']
            row_dict = {'period_cd': period_cd, 'period_year': period_year, 'period_num': period_num, 'add_type': add_type,
                        'add_year': add_year, 'add_num': add_num}
            acc_cal = AccumulateCalendar(**row_dict)
            acc_cal_dic[acc_key].append(acc_cal)
        gv.set_run_var_value('ACC_CAL_DIC', acc_cal_dic)

    if acc_cal_dic is None:
        return None, None
    elif acc_key not in acc_cal_dic:
        return None, None
    else:
        acc_cal_lst = acc_cal_dic[acc_key]
    for acc in acc_cal_lst:
        if add_type == acc.add_type and period_num == acc.period_num \
                and period_year == acc.period_year and period_cd == acc.period_cd:
            return acc.add_year, acc.add_num


def get_prd_year_seq(add_type, period_cd, acc_year, acc_no):
    """
    获取期间年度和期间序号
    Created by David on 2019/01/15
    :param add_type: 累计类型
    :param period_cd: 期间编码
    :param acc_year: 累计年度
    :param acc_no: 累计序号
    :return:(期间年度,期间序号)组成的list
    """

    catalog = gv.get_run_var_value('PY_CATALOG')
    tenant_id = catalog.tenant_id

    dic_key = str(add_type) + str(period_cd) + str(acc_year) + str(acc_no)
    prd_yr_seq_dic = gv.get_run_var_value('PRD_YEAR_SEQ_DIC')
    if prd_yr_seq_dic is None:
        prd_yr_seq_dic = dict()
        prd_yr_seq_dic[dic_key] = []
    else:
        if dic_key not in prd_yr_seq_dic:
            prd_yr_seq_dic[dic_key] = []
    if len(prd_yr_seq_dic[dic_key]) == 0:
        db = gv.get_db()
        stmt = text("select hhr_period_year, hhr_prd_num from boogoo_payroll.hhr_py_period_add_calendar_line where tenant_id = :b1 "
                    "and hhr_period_code = :b2 and hhr_period_add_type = :b3 and hhr_period_add_year = :b4 and hhr_period_add_number = :b5 ")
        rs = db.conn.execute(stmt, b1=tenant_id, b2=period_cd, b3=add_type, b4=acc_year, b5=acc_no).fetchall()
        for row in rs:
            period_year = row['hhr_period_year']
            period_num = row['hhr_prd_num']
            prd_yr_seq_dic[dic_key].append((period_year, period_num))
        gv.set_run_var_value('PRD_YEAR_SEQ_DIC', prd_yr_seq_dic)
    return prd_yr_seq_dic[dic_key]


def get_prd_date_lst(period_cd, period_year, period_num):
    """
    获取期间开始日期和结束日期
    Created by David on 2019/01/15
    :param period_cd: 期间编码
    :param period_year: 期间年度
    :param period_num: 期间序号
    :return:[开始日期,结束日期,上一期间年度,上一期间序号]
    """

    catalog = gv.get_run_var_value('PY_CATALOG')
    tenant_id = catalog.tenant_id
    db = gv.get_db()

    prd_cal_date_dic = gv.get_run_var_value('PRD_CAL_DATE_DIC')
    key_tuple = (period_cd, period_year, period_num)
    if key_tuple not in prd_cal_date_dic:
        prd_cal_date_dic[key_tuple] = []
        sql = text("select hhr_period_start_date, hhr_period_end_date, hhr_period_last_year, hhr_last_prd_num from boogoo_payroll.hhr_py_period_calendar_line "
                   "where tenant_id = :b1 and hhr_period_code = :b2 and hhr_period_year = :b3 and hhr_prd_num = :b4 ")
        row = db.conn.execute(sql, b1=tenant_id, b2=period_cd, b3=period_year, b4=period_num).fetchone()
        prd_bgn_date = row['hhr_period_start_date']
        prd_end_date = row['hhr_period_end_date']
        prd_last_year = row['hhr_period_last_year']
        last_prd_num = row['hhr_last_prd_num']
        prd_cal_date_dic[key_tuple].append(prd_bgn_date)
        prd_cal_date_dic[key_tuple].append(prd_end_date)
        prd_cal_date_dic[key_tuple].append(prd_last_year)
        prd_cal_date_dic[key_tuple].append(last_prd_num)
    prd_info_lst = prd_cal_date_dic[key_tuple]
    return prd_info_lst


def set_pin_seg_amt(pin_cd, amt, segment_num='*', **kwargs):
    """
    设置薪资项目某个分段的金额
    Created by David on 2018/09/18
    :param pin_cd: 薪资项目代码
    :param amt: 金额
    :param segment_num: 分段号，不分段则传入'*'
    :param kwargs: currency/round_rule_id/ratio/override
    :return: None
    """

    currency = kwargs.get('currency', None)
    round_rule_id = kwargs.get('round_rule_id', None)
    ratio = kwargs.get('ratio', None)
    override = kwargs.get('override', True)

    pins_dic = gv.get_pin_dic()
    pin_obj = pins_dic[pin_cd]
    if pin_obj.prcs_flag in ['C', 'U', 'S']:
        return
    if round_rule_id is not None:
        pin_obj.segment[segment_num].round_rule_id = round_rule_id
        if override:
            pin_obj.segment[segment_num].amt_ = amt
        else:
            pin_obj.segment[segment_num].amt_ += amt
        if currency is not None:
            pin_obj.segment[segment_num].currency = currency
        # pin_obj.segment[segment_num].std_amt += amt
        if ratio is not None:
            pin_obj.segment[segment_num].ratio = ratio
    else:
        if override:
            pin_obj.segment[segment_num].amt = amt
        else:
            pin_obj.segment[segment_num].amt += amt
        if currency is not None:
            pin_obj.segment[segment_num].currency = currency
        # pin_obj.segment[segment_num].std_amt += amt
        if ratio is not None:
            pin_obj.segment[segment_num].ratio = ratio


def ex_currency(from_cur, to_cur, from_amt):
    """
    货币转换
    Created by David on 2018/09/18
    :param from_cur: 源货币
    :param to_cur: 目标货币
    :param from_amt: 源货币金额
    :return:to_amt 目标货币金额
    """

    catalog = gv.get_run_var_value('PY_CATALOG')
    f_end_date = catalog.f_prd_end_dt
    # if not isinstance(from_amt, Decimal) and not isinstance(from_amt, float) and not isinstance(from_amt, int):
    #     return 0
    if from_cur == to_cur:
        return from_amt
    if from_amt == 0:
        return 0
    tenant_id = catalog.tenant_id
    ex_rate_dic = gv.get_run_var_value('EX_RATE_DIC')
    if f_end_date in ex_rate_dic:
        ex_rate_lst = ex_rate_dic[f_end_date]
        for rate in ex_rate_lst:
            if rate.from_cur == from_cur and rate.to_cur == to_cur and rate.tenant_id == tenant_id:
                r_from_amt = rate.from_amt
                r_to_amt = rate.to_amt
                if r_from_amt == 0:
                    return 0
                decimal_value = Decimal(str(r_to_amt / r_from_amt * from_amt))
                to_amt = Decimal(decimal_value).quantize(Decimal('0.0000'))
                return to_amt
    else:
        return 0


def get_area_sal_lvl(end_dt=None, currency=None):
    """
    获取缴纳地工资水平
    Created by David on 2018/09/18
    :param end_dt 结束日期
    :param currency: 货币
    :return: 缴纳地工资水平字典，key为元组(地区, 年度, 生效日期)
    """

    db = gv.get_db()
    catalog = gv.get_run_var_value('PY_CATALOG')
    tenant_id = catalog.tenant_id

    if end_dt is None:
        end_dt = catalog.f_prd_end_dt

    area_sal_lvl_dic = gv.get_run_var_value('AREA_SAL_LVL_DIC')
    if area_sal_lvl_dic is None:
        area_sal_lvl_dic = dict()

    if end_dt in area_sal_lvl_dic:
        return area_sal_lvl_dic[end_dt]
    else:
        temp_dic = {}
        sql = text(
            "select a.hhr_efft_date, a.hhr_area_code,a.hhr_year,a.hhr_avg_anl_sal,a.hhr_avg_mon_sal,a.hhr_min_mon_sal, "
            "a.hhr_currency from boogoo_payroll.hhr_py_area_sal_lvl a where a.tenant_id = :b1 "
            "and a.hhr_efft_date <= :b2 order by a.hhr_area_code,a.hhr_year asc,a.hhr_efft_date asc")
        rs = db.conn.execute(sql, b1=tenant_id, b2=end_dt).fetchall()
        for row in rs:
            eff_dt = row['hhr_efft_date']
            area_id = row['hhr_area_code']
            year = row['hhr_year']
            avg_anl_sal = row['hhr_avg_anl_sal']
            avg_mon_sal = row['hhr_avg_mon_sal']
            min_mon_sal = row['hhr_min_mon_sal']
            row_currency = row['hhr_currency']

            if currency is not None and row_currency != currency:
                avg_anl_sal = ex_currency(row_currency, currency, avg_anl_sal)
                avg_mon_sal = ex_currency(row_currency, currency, avg_mon_sal)
                min_mon_sal = ex_currency(row_currency, currency, min_mon_sal)

            temp_dic[(area_id, year, eff_dt)] = {'avg_anl_sal': avg_anl_sal, 'avg_mon_sal': avg_mon_sal,
                                                 'min_mon_sal': min_mon_sal, 'currency': currency,
                                                 'pg_currency': currency}
        area_sal_lvl_dic[end_dt] = temp_dic
        gv.set_run_var_value('AREA_SAL_LVL_DIC', area_sal_lvl_dic)
        return area_sal_lvl_dic[end_dt]


def get_max_avg_anl_sal(area, f_period_year, currency):
    """
    获取生效日期最大的记录对应的平均年度工资
    Created by David on 2018/09/18
    :param area: 缴纳地
    :param f_period_year: 历经期期间年度
    :param currency: 货币
    :return avg_anl_sal: 平均年度工资
    """

    # catalog = gv.get_run_var_value('PY_CATALOG')
    # f_end_date = catalog.f_prd_end_dt

    eff_dt_lst = []
    avg_anl_sal = 0
    area_sal_data_dic = get_area_sal_lvl(currency=currency)
    for tuple_key, value_dic in area_sal_data_dic.items():
        if tuple_key[0] == area and tuple_key[1] == f_period_year:
            eff_dt_lst.append(tuple_key[2])
    if len(eff_dt_lst) > 0:
        eff_dt = eff_dt_lst[len(eff_dt_lst) - 1]
        area_sal_val_dic = area_sal_data_dic[(area, f_period_year, eff_dt)]
        avg_anl_sal = area_sal_val_dic['avg_anl_sal']
    if avg_anl_sal is None:
        avg_anl_sal = 0
    return avg_anl_sal


def get_min_mon_sal(area, year, end_dt):
    """
    获取最低工资
    Created by David on 2019/01/07
    :param area: 缴纳地
    :param year: 年度
    :param end_dt: 结束日期
    :return:
    """

    ret_obj = ReturnObj()
    eff_dt_lst = []
    area_sal_data_dic = get_area_sal_lvl(end_dt=end_dt)
    for tuple_key, value_dic in area_sal_data_dic.items():
        if tuple_key[0] == area and tuple_key[1] == year:
            eff_dt_lst.append(tuple_key[2])
    if len(eff_dt_lst) > 0:
        eff_dt = eff_dt_lst[len(eff_dt_lst) - 1]
        area_sal_val_dic = area_sal_data_dic[(area, year, eff_dt)]
        avg_anl_sal = area_sal_val_dic['avg_anl_sal']
        avg_mon_sal = area_sal_val_dic['avg_mon_sal']
        min_mon_sal = area_sal_val_dic['min_mon_sal']
        currency = area_sal_val_dic['currency']
        ret_obj.avg_anl_sal = avg_anl_sal
        ret_obj.avg_mon_sal = avg_mon_sal
        ret_obj.min_mon_sal = min_mon_sal
        ret_obj.currency = currency

    return ret_obj


def get_db_pin_by_catalog(**kwargs):
    """
    根据参数在数据库中查询薪资项目的值
    :param kwargs:
    :return:
    """
    catalog = kwargs.get('catalog', None)
    pin_code_list = kwargs.get('pin_code_list', '')
    seq_num = kwargs.get('seqnum', -1)
    return_dic = dict()

    db = gv.get_db()
    pin_string = "('x1@!#'"
    for pin_code in pin_code_list:
        pin_string += ",'" + pin_code + "'"
    pin_string += ")"
    sql = text("select a.hhr_pin_cd, a.hhr_amt from boogoo_payroll.hhr_py_rslt_pin a where a.tenant_id=:b1 and a.hhr_empid=:b2 "
               "and a.hhr_emp_rcd=:b3 and a.hhr_seq_num=:b4 and a.hhr_pin_cd in " + pin_string)
    result = db.conn.execute(sql, b1=catalog.tenant_id, b2=catalog.emp_id, b3=catalog.emp_rcd,
                             b4=seq_num).fetchall()

    if result is not None:
        for result_line in result:
            pin_cd = result_line['hhr_pin_cd']
            return_dic[pin_cd] = return_dic.get(pin_cd, 0) + result_line['hhr_amt']
    return return_dic


def get_db_acc_by_catalog(**kwargs):
    """
    根据参数在数据库中查询累计项目的值(指定累计编码，类型就是固定的了，一个编码不会有2个类型)
    :param kwargs:
    :return:
    """
    catalog = kwargs.get('catalog', None)
    acc_code_list = kwargs.get('acc_code_list', '')
    seq_num = kwargs.get('seqnum', -1)
    return_dic = dict()

    db = gv.get_db()
    acc_string = "('x1@!#'"
    for acc_code in acc_code_list:
        acc_string += ",'" + acc_code + "'"
    acc_string += ")"
    sql = text("select a.hhr_acc_cd, a.hhr_accm_type, a.hhr_period_add_year, a.hhr_period_add_number, "
               "a.hhr_amt, a.hhr_currency from boogoo_payroll.hhr_py_rslt_accm a where a.tenant_id=:b1 and a.hhr_empid=:b2 "
               "and a.hhr_emp_rcd=:b3 and a.hhr_seq_num=:b4 and a.hhr_acc_cd in " + acc_string)
    result = db.conn.execute(sql, b1=catalog.tenant_id, b2=catalog.emp_id, b3=catalog.emp_rcd,
                             b4=seq_num).fetchall()

    if result is not None:
        for result_line in result:
            acc_cd = result_line['hhr_acc_cd']
            acc_type = result_line['hhr_accm_type']
            acc_year = result_line['hhr_period_add_year']
            acc_num = result_line['hhr_period_add_number']
            amt = result_line['hhr_amt']
            currency = result_line['hhr_currency']
            return_dic[acc_cd] = [acc_type, acc_year, acc_num, amt, currency]
    return return_dic


def get_db_var_by_catalog(**kwargs):
    """
    根据参数在数据库中查询变量的值
    :param kwargs:
    :return:
    """
    catalog = kwargs.get('catalog', None)
    var_code_list = kwargs.get('var_code_list', '')
    seq_num = kwargs.get('seqnum', -1)
    return_dic = dict()

    db = gv.get_db()
    var_string = "('x1@!#'"
    for var_code in var_code_list:
        var_string += ",'" + var_code + "'"
    var_string += ")"
    sql = text("select a.hhr_variable_id, a.hhr_data_type, a.hhr_varval_char, a.hhr_varval_date, a.hhr_varval_dec "
               "from boogoo_payroll.hhr_py_rslt_var a where a.tenant_id=:b1 and a.hhr_empid=:b2 "
               "and a.hhr_emp_rcd=:b3 and a.hhr_seq_num=:b4 and a.hhr_variable_id in " + var_string)
    result = db.conn.execute(sql, b1=catalog.tenant_id, b2=catalog.emp_id, b3=catalog.emp_rcd,
                             b4=seq_num).fetchall()

    if result is not None:
        for result_line in result:
            var_cd = result_line['hhr_variable_id']
            data_type = result_line['hhr_data_type']
            var_char = result_line['hhr_varval_char']
            var_date = result_line['hhr_varval_date']
            var_dec = result_line['hhr_varval_dec']
            if data_type == "string":
                value = var_char
            elif data_type == "float":
                value = var_dec
            elif data_type == "datetime":
                value = var_date
            else:
                value = None
            return_dic[var_cd] = value
    return return_dic
