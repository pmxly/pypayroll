# -*- coding: utf-8 -*-


from sqlalchemy.sql import text
from copy import copy
from .excelutils.create_excel import create_py_detail_excl, create_py_sum_excl


def get_default_tmpl_cd(conn, tenant_id, tmpl_type):
    """
    Desc: 获取默认模版
    Author: David
    Date: 2019/03/01
    """

    tmpl_cd = ''
    s1 = text("select HHR_TMPL_CD from hhr_py_rept_tmpl_b where tenant_id = :b1 and HHR_TMPL_TYPE = :b2 AND HHR_DEFAULT = 'Y' ")
    row = conn.execute(s1, b1=tenant_id, b2=tmpl_type).fetchone()
    if row is not None:
        tmpl_cd = row['HHR_TMPL_CD']
    return tmpl_cd


def get_rept_tmpl_lines(conn, tenant_id, lang, tmpl_cd):
    """
    Desc: 查询模板列
    Author: 陶雨
    Date: 2019/01/17
    """

    result = []

    s1 = text(
        "SELECT A.HHR_TMPL_LINE_CD, A1.HHR_TMPL_LINE_TAB, A.HHR_TABLE_FROM, A.HHR_FIELD, A.HHR_FIELD_VAL, A.HHR_SEQ_BETW "
        "FROM hhr_py_rept_tmpl_line_b A LEFT JOIN hhr_py_rept_tmpl_line_tl A1 ON A.tenant_id = A1.tenant_id " 
        "AND A.HHR_TMPL_CD = A1.HHR_TMPL_CD AND A.HHR_TMPL_LINE_CD = A1.HHR_TMPL_LINE_CD AND A1.LANG = :b2 "
        "WHERE A.tenant_id = :b1 AND A.HHR_TMPL_CD = :b3 order by A.HHR_TMPL_LINE_SORT "
    )
    rs = conn.execute(s1, b1=tenant_id, b2=lang, b3=tmpl_cd).fetchall()
    for row1 in rs:
        item_dic = {}
        field_name = row1['HHR_FIELD']
        tmpl_line_cd = row1['HHR_TMPL_LINE_CD']
        tmpl_line_tab = row1['HHR_TMPL_LINE_TAB']
        table_from = row1['HHR_TABLE_FROM']
        field_val = row1['HHR_FIELD_VAL']
        seq_betw = row1['HHR_SEQ_BETW']
        item_dic[field_name] = [tmpl_line_cd, tmpl_line_tab, table_from, field_val, seq_betw]
        result.append(item_dic)
    return result


def get_fcal_equal_pycal(conn, tenant_id, py_cal_id, emp_id, emp_rcd):
    """
    Desc: 查询所在期日历等于历经期日历的数据（传日历参数）
    Author: 陶雨
    Date: 2019/01/17
    """

    sql_catalog = "select a.hhr_empid, b.hhr_emp_name, a.hhr_emp_rcd, a.hhr_seq_num, " \
                  "a.hhr_f_cal_id, a.hhr_py_cal_id, a.hhr_pygroup_id, a.hhr_prd_bgn_dt, a.hhr_prd_end_dt " \
                  "from hhr_payroll.hhr_py_cal_catalog a left join hhr_corehr.hhr_org_per_biog b on a.tenant_id = b.tenant_id and a.hhr_empid = b.hhr_empid " \
                  "where a.tenant_id = :b1 and a.hhr_f_cal_id = a.hhr_py_cal_id and a.hhr_f_cal_id = :b2 "

    # 如果传递了员工编码参数
    if emp_id:
        sql_catalog = sql_catalog + ' and a.hhr_empid = :b3 and a.hhr_emp_rcd = :b4 '

    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=py_cal_id, b3=emp_id, b4=emp_rcd).fetchall()

    return rs


def get_fcal_unequal_pycal(conn, tenant_id, py_cal_id, emp_id, emp_rcd):
    """
    Desc: 查询所在期日历不等于历经期日历的数据（传日历参数）
    Author: 陶雨
    Date: 2019/01/17
    """

    sql_catalog = "select a.hhr_empid, b.hhr_emp_name, a.hhr_emp_rcd, a.hhr_seq_num, " \
                  "a.hhr_f_cal_id, a.hhr_py_cal_id, a.hhr_pygroup_id, a.hhr_prd_bgn_dt, a.hhr_prd_end_dt, a.hhr_f_period_year, a.hhr_f_prd_num " \
                  "from hhr_payroll.hhr_py_cal_catalog a left join hhr_corehr.hhr_org_per_biog b on a.tenant_id = b.tenant_id and a.hhr_empid = b.hhr_empid " \
                  "where a.tenant_id = :b1 and a.hhr_f_cal_id <> a.hhr_py_cal_id and a.hhr_py_cal_id = :b2 "

    sql_catalog2 = "order by a.hhr_empid, a.hhr_emp_rcd, a.hhr_f_period_year, a.hhr_f_prd_num "

    # 如果传递了员工编码参数
    if emp_id:
        sql_catalog = sql_catalog + ' and a.hhr_empid = :b3 and a.hhr_emp_rcd = :b4 ' + sql_catalog2
    else:
        sql_catalog = sql_catalog + sql_catalog2

    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=py_cal_id, b3=emp_id, b4=emp_rcd).fetchall()

    return rs


def get_fcal_equal_pycal_withoutcal(conn, user_id, tenant_id, start_date, end_date, emp_id, emp_rcd, py_group_id):
    """
    Desc: 查询所在期日历等于历经期日历的数据（不传日历参数）
    Author: 陶雨
    Date: 2019/01/23
    """

    sql_catalog = "SELECT A.HHR_EMPID, B.hhr_emp_name, A.HHR_EMP_RCD, A.HHR_SEQ_NUM, " \
                  "A.HHR_F_CAL_ID, A.HHR_PY_CAL_ID, A.HHR_PYGROUP_ID, A.HHR_PRD_BGN_DT, A.HHR_PRD_END_DT " \
                  "FROM hhr_payroll.hhr_py_cal_catalog A LEFT JOIN hhr_per_biographical_b B ON A.tenant_id = B.tenant_id AND A.HHR_EMPID = B.HHR_EMPID " \
                  "JOIN vw_hhr_cls_paygroup_basesal C ON A.tenant_id = C.tenant_id AND A.HHR_PYGROUP_ID = C.HHR_PYGROUP_ID " \
                  "AND C.USER_NAME = :b1 " \
                  "WHERE A.tenant_id = :b2 AND A.HHR_F_CAL_ID = A.HHR_PY_CAL_ID AND A.HHR_PY_REC_STAT <> 'O' " \
                  "AND A.HHR_PRD_END_DT >= :b3 AND A.HHR_PRD_BGN_DT <= :b4 "

    sql_catalog2 = "ORDER BY A.HHR_EMPID, A.HHR_EMP_RCD, A.HHR_SEQ_NUM "

    # 如果传递了员工编码参数
    if emp_id:
        sql_catalog = sql_catalog + ' AND A.HHR_EMPID = :b5 AND A.HHR_EMP_RCD = :b6 '

    # 如果传递了薪资组
    if py_group_id:
        sql_catalog = sql_catalog + ' AND A.HHR_PYGROUP_ID = :b7 '

    sql_catalog = sql_catalog + sql_catalog2

    rs = conn.execute(text(sql_catalog), b1=user_id, b2=tenant_id, b3=start_date, b4=end_date, b5=emp_id, b6=emp_rcd, b7=py_group_id).fetchall()

    return rs


def get_relt_array_by_seq(conn, sql_field_string, tenant_id, lang, seq_num, emp_id, emp_rcd, dept_cd, eff_dt, has_child_dept):
    """
    Desc: 根据序号取可写数组表中每一列的值
    Author: 陶雨
    Date: 2019/01/17
    """

    sql_catalog1 = "select a.hhr_empid, a.hhr_emp_rcd, a.hhr_seq_num, b.hhr_dept_name, fu1.hhr_dept_detail_desc, c.hhr_position_name, " + sql_field_string + " FROM hhr_payroll.hhr_py_rslt_array_01 A "

    sql_catalog2 = "LEFT JOIN hhr_org_dept_tl B ON B.HHR_SYS_CODE = A.HHR_SYS_CODE AND B.HHR_DEPT_CODE = A.HHR_TXT001 AND B.LANG = :b6 " \
                   "AND B.hhr_efft_date =  (SELECT max(hhr_efft_date) FROM hhr_org_dept_b B1 WHERE B1.HHR_SYS_CODE = B.HHR_SYS_CODE  " \
                   "AND B1.HHR_DEPT_CODE = B.HHR_DEPT_CODE AND B1.hhr_efft_date <= :b7 ) " \
                   "LEFT JOIN hhr_org_posn_tl C ON C.HHR_SYS_CODE = A.HHR_SYS_CODE AND C.HHR_POSITION_CODE = A.HHR_TXT002 AND C.LANG = :b6 " \
                   "AND C.hhr_efft_date =  (SELECT max(hhr_efft_date) FROM hhr_org_posn_b C1 WHERE C1.HHR_SYS_CODE = C.HHR_SYS_CODE  " \
                   "AND C1.HHR_POSITION_CODE = C.HHR_POSITION_CODE AND C1.hhr_efft_date <= :b7 ) " \
                   "LEFT JOIN hhr_dept_full_desc fu1 ON fu1.HHR_SYS_CODE = B.HHR_SYS_CODE AND fu1.HHR_DEPT_CODE = B.HHR_DEPT_CODE " \
                   "AND fu1.hhr_efft_date = (SELECT max(fu2.hhr_efft_date) FROM hhr_dept_full_desc fu2 " \
                   "  WHERE fu2.HHR_SYS_CODE = fu1.HHR_SYS_CODE AND fu2.HHR_DEPT_CODE = fu1.HHR_DEPT_CODE " \
                   "    AND fu2.hhr_efft_date <= :b7) AND fu1.LANG = B.LANG "

    sql_catalog3 = "WHERE A.HHR_SYS_CODE = :b1 AND A.HHR_EMPID = :b2 AND A.HHR_EMP_RCD = :b3 AND A.HHR_SEQ_NUM = :b4 "

    sql_catalog4 = "ORDER BY A.HHR_EMPID, A.HHR_EMP_RCD, A.HHR_SEQ_NUM "

    # 如果传递了部门参数
    if dept_cd:
        if has_child_dept == 'Y':
            sql_catalog = sql_catalog1 + sql_catalog2 + sql_catalog3 + ' AND (A.HHR_TXT001 = :b5 OR' \
                                                                       ' A.HHR_TXT001 IN (SELECT A1.HHR_DEPT_CODE FROM hhr_dept_lvl A1 ' \
                                                                       ' WHERE A1.HHR_SYS_CODE = :b1 AND A1.HHR_DEPT_HIGHER_DEPT = :b5 ' \
                                                                       'and A1.hhr_efft_date = (select max(A2.hhr_efft_date) from hhr_dept_lvl A2 ' \
                                                                       '    where A2.HHR_SYS_CODE = A1.HHR_SYS_CODE and A2.HHR_DEPT_CODE = A1.HHR_DEPT_CODE ' \
                                                                       '      and A2.hhr_efft_date <= :b7)  ' \
                                                                       ')) ' + sql_catalog4
        else:
            sql_catalog = sql_catalog1 + sql_catalog2 + sql_catalog3 + ' AND A.HHR_TXT001 = :b5 ' + sql_catalog4
    else:
        sql_catalog = sql_catalog1 + sql_catalog2 + sql_catalog3 + sql_catalog4

    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=dept_cd, b6=lang, b7=eff_dt).fetchone()

    return rs


def get_retro_amt(conn, tenant_id, emp_id, emp_rcd, seq_num, py_elem_id):
    """
    Desc: 查询追溯金额
    Author: 陶雨
    Date: 2019/01/21
    """
    sql_catalog = "SELECT A.HHR_EMPID, A.HHR_EMP_RCD, A.HHR_SEQ_NUM, A.HHR_PIN_CD, A.HHR_RETRO_AMT " \
                  "FROM hhr_py_rslt_pin A WHERE A.HHR_SYS_CODE = :b1 AND A.HHR_EMPID = :b2 AND A.HHR_EMP_RCD = :b3 " \
                  "AND A.HHR_SEQ_NUM = :b4 AND A.HHR_PIN_CD = :b5 " \
                  "AND A.HHR_SEGMENT_NUM = (SELECT MAX(A1.HHR_SEGMENT_NUM) FROM hhr_py_rslt_pin A1 " \
                  "WHERE A.HHR_SYS_CODE = A1.HHR_SYS_CODE AND A.HHR_EMPID = A1.HHR_EMPID " \
                  "AND A.HHR_EMP_RCD = A1.HHR_EMP_RCD AND A.HHR_SEQ_NUM = A1.HHR_SEQ_NUM AND A.HHR_PIN_CD = A1.HHR_PIN_CD)"
    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=py_elem_id).fetchone()
    return rs


def get_high_dept(conn, tenant_id, dept_cd, lang, effe_dt):
    """
    Desc: 查询部门的上级部门
    Author: 陶雨
    Date: 2019/01/26
    """

    sql_catalog = "SELECT A.HHR_DEPT_CODE, A.HHR_DEPT_HIGHER_DEPT, B.HHR_DEPT_NAME, A.HHR_RPT_LVL " \
                  "FROM hhr_dept_lvl A LEFT JOIN hhr_org_dept_tl B ON B.HHR_SYS_CODE = A.HHR_SYS_CODE " \
                  "AND B.HHR_DEPT_CODE = A.HHR_DEPT_CODE AND B.LANG = :b3 AND B.hhr_efft_date = (SELECT max(hhr_efft_date) " \
                  "FROM hhr_org_dept_b B1 WHERE B1.HHR_SYS_CODE = B.HHR_SYS_CODE AND B1.HHR_DEPT_CODE = B.HHR_DEPT_CODE AND B1.HHR_EFFE_DATE <= :b4 ) " \
                  "WHERE A.HHR_SYS_CODE = :b1 AND A.HHR_DEPT_CODE = :b2 AND A.HHR_RPT_LVL <> 0 ORDER BY A.HHR_RPT_LVL DESC "

    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=dept_cd, b3=lang, b4=effe_dt).fetchall()

    return rs


def merge_dic_val(dic_a, dic_b):
    """
    Desc: 合并字典
    Author: 陶雨
    Date: 2019/01/22
    """

    dic = {}
    for key in dic_a:
        if dic_b.get(key):
            dic[key] = dic_a[key] + dic_b[key]
        else:
            dic[key] = dic_a[key]
    for key in dic_b:
        if dic_a.get(key):
            pass
        else:
            dic[key] = dic_b[key]
    return dic


def get_cal_lst_in_date_range(conn, user_id, tenant_id, start_date, end_date, emp_id, emp_rcd, py_group_id):

    cal_lst = []
    sql_catalog = "SELECT distinct A.HHR_PY_CAL_ID FROM hhr_py_cal_catalog A " \
                  "JOIN vw_hhr_cls_paygroup_basesal C ON A.HHR_SYS_CODE = C.HHR_SYS_CODE AND A.HHR_PYGROUP_ID = C.HHR_PYGROUP_ID " \
                  "AND C.USER_NAME = :b1 " \
                  "WHERE A.HHR_SYS_CODE = :b2 AND A.HHR_F_CAL_ID = A.HHR_PY_CAL_ID AND A.HHR_PY_REC_STAT <> 'O' " \
                  "AND A.HHR_PRD_END_DT >= :b3 AND A.HHR_PRD_BGN_DT <= :b4 "

    # 如果传递了员工编码参数
    if emp_id:
        sql_catalog = sql_catalog + ' AND A.HHR_EMPID = :b5 AND A.HHR_EMP_RCD = :b6 '
    # 如果传递了薪资组
    if py_group_id:
        sql_catalog = sql_catalog + ' AND A.HHR_PYGROUP_ID = :b7 '
    rs = conn.execute(text(sql_catalog), b1=user_id, b2=tenant_id, b3=start_date, b4=end_date, b5=emp_id, b6=emp_rcd, b7=py_group_id).fetchall()
    for row in rs:
        py_cal_id = row['HHR_PY_CAL_ID']
        cal_lst.append(py_cal_id)
    return cal_lst


def get_all_periods_by_cal(conn, tenant_id, py_cal_id, emp_id, emp_rcd, periods_lst):
    sql_catalog = "SELECT distinct A.HHR_F_PERIOD_YEAR, A.HHR_F_PRD_NUM FROM hhr_py_cal_catalog A " \
                  "WHERE A.HHR_SYS_CODE = :b1 AND A.HHR_F_CAL_ID <> A.HHR_PY_CAL_ID AND A.HHR_PY_CAL_ID = :b2 "

    sql_catalog2 = "order by A.HHR_F_PERIOD_YEAR, A.HHR_F_PRD_NUM "

    # 如果传递了员工编码参数
    if emp_id:
        sql_catalog = sql_catalog + ' AND A.HHR_EMPID = :b3 AND A.HHR_EMP_RCD = :b4 ' + sql_catalog2
    else:
        sql_catalog = sql_catalog + sql_catalog2
    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=py_cal_id, b3=emp_id, b4=emp_rcd).fetchall()
    for row in rs:
        f_prd_year_num = str(row['HHR_F_PERIOD_YEAR']) + str(row['HHR_F_PRD_NUM'])
        if len(f_prd_year_num) == 5:
            f_prd_year_num = f_prd_year_num[:4] + '0' + f_prd_year_num[4:]
        if f_prd_year_num not in periods_lst:
            periods_lst.append(f_prd_year_num)


def get_cal_retro_data(run_param, py_cal_id, tmpl_line_list, m_prd_dic):
    conn = run_param['conn']
    sys_code = run_param['sys_code']
    in_emp_id = run_param['emp_id']
    in_emp_rcd = run_param['emp_rcd']

    catalog_unequal_list = get_fcal_unequal_pycal(conn, sys_code, py_cal_id, in_emp_id, in_emp_rcd)
    f_prd_dic = {}
    if catalog_unequal_list is not None:
        for row in catalog_unequal_list:
            f_prd_year_num = str(row['HHR_F_PERIOD_YEAR']) + str(row['HHR_F_PRD_NUM'])
            if len(f_prd_year_num) == 5:
                f_prd_year_num = f_prd_year_num[:4] + '0' + f_prd_year_num[4:]
            seq_num = row['HHR_SEQ_NUM']
            emp_id = row['HHR_EMPID']
            emp_rcd = row['HHR_EMP_RCD']

            f_prd_key = (emp_id, emp_rcd, py_cal_id, f_prd_year_num)
            f_prd_dic[f_prd_key] = {}
            for item_dic in tmpl_line_list:
                for field_info_lst in item_dic.values():
                    py_elem_id = field_info_lst[3]
                    if py_elem_id:
                        retro_data = get_retro_amt(conn, sys_code, emp_id, emp_rcd, seq_num, py_elem_id)
                        if retro_data is not None:
                            f_prd_dic[f_prd_key][py_elem_id] = retro_data['HHR_RETRO_AMT']
            if f_prd_key not in m_prd_dic:
                m_prd_dic[f_prd_key] = copy(f_prd_dic[f_prd_key])
            else:
                d1, d2 = m_prd_dic[f_prd_key], f_prd_dic[f_prd_key]
                m_prd_dic[f_prd_key] = merge_dic_val(d1, d2)


def get_fld_info_tup(tmpl_line_list, f_prd_year_num_list, by_trac_detail):
    # 获取所有字段标签以及字段列表
    field_label_lst = []
    sql_field_list = []
    for item_dic in tmpl_line_list:
        for field_name, field_info_lst in item_dic.items():
            field_label = field_info_lst[1]
            py_elem_id = field_info_lst[3]

            sql_field_list.append(field_name)

            if field_name == 'HHR_TXT001':
                field_label = field_label + '##HHR_TXT001'
                field_label_lst.append(field_label)
            elif field_name == 'HHR_TXT002':
                field_label = field_label + '##HHR_TXT002'
                field_label_lst.append(field_label)
            else:
                field_label_lst.append(field_label)

            if by_trac_detail:
                # 如果包含薪资项，则显示追溯列
                if py_elem_id:
                    for f_prd_year_num in f_prd_year_num_list:
                        fld_prd_label = field_label + '_' + f_prd_year_num
                        field_label_lst.append(fld_prd_label)
    return field_label_lst, sql_field_list


def get_prd_year_info_tup(run_param, tmpl_line_list):
    conn = run_param['conn']
    sys_code = run_param['sys_code']
    py_cal_id = run_param['py_cal_id']
    in_emp_id = run_param['emp_id']
    in_emp_rcd = run_param['emp_rcd']

    catalog_unequal_list = get_fcal_unequal_pycal(conn, sys_code, py_cal_id, in_emp_id, in_emp_rcd)
    f_prd_dic = {}
    m_prd_dic = {}
    f_prd_year_num_list = []
    if catalog_unequal_list is not None:
        # 获取每个员工每个追溯期间内各薪资项的金额
        for row in catalog_unequal_list:
            # 拼接期间后缀
            f_prd_year_num = str(row['HHR_F_PERIOD_YEAR']) + str(row['HHR_F_PRD_NUM'])
            if len(f_prd_year_num) == 5:
                f_prd_year_num = f_prd_year_num[:4] + '0' + f_prd_year_num[4:]
            f_prd_year_num_list.append(f_prd_year_num)
            seq_num = row['HHR_SEQ_NUM']
            emp_id = row['HHR_EMPID']
            emp_rcd = row['HHR_EMP_RCD']

            f_prd_key = (emp_id, emp_rcd, f_prd_year_num)
            f_prd_dic[f_prd_key] = {}
            for item_dic in tmpl_line_list:
                for field_info_lst in item_dic.values():
                    py_elem_id = field_info_lst[3]
                    if py_elem_id:
                        retro_data = get_retro_amt(conn, sys_code, emp_id, emp_rcd, seq_num, py_elem_id)
                        if retro_data is not None:
                            # # 判断是否有重复key
                            # if f_prd_key not in f_prd_dic:
                            #     f_prd_dic[f_prd_key] = {py_elem_id: retro_data['HHR_RETRO_AMT']}
                            # else:
                            #     f_prd_dic[f_prd_key][py_elem_id] = retro_data['HHR_RETRO_AMT']
                            f_prd_dic[f_prd_key][py_elem_id] = retro_data['HHR_RETRO_AMT']
            if f_prd_key not in m_prd_dic:
                m_prd_dic[f_prd_key] = copy(f_prd_dic[f_prd_key])
            else:
                d1, d2 = m_prd_dic[f_prd_key], f_prd_dic[f_prd_key]
                m_prd_dic[f_prd_key] = merge_dic_val(d1, d2)

        # 去重+排序
        f_prd_year_num_list = sorted(set(f_prd_year_num_list))
    return m_prd_dic, f_prd_year_num_list


def build_upper_lvl_dept_dic(conn, tenant_id, lang, prd_bgn_dt, prd_end_dt, dept_id, dept_desc, high_dept_dic):
    """返回部门的所有上级部门列表"""

    tup_key = (prd_bgn_dt, prd_end_dt, dept_id)
    if tup_key not in high_dept_dic:
        high_dept_dic[tup_key] = []
        high_dept_dic[tup_key].append((dept_id, dept_desc))
        sql_catalog = text("SELECT A.HHR_DEPT_HIGHER_DEPT, B.HHR_DEPT_NAME "
                           "FROM hhr_dept_lvl A LEFT JOIN hhr_org_dept_tl B ON B.HHR_SYS_CODE = A.HHR_SYS_CODE "
                           "AND B.HHR_DEPT_CODE = A.HHR_DEPT_HIGHER_DEPT AND B.LANG = :b3 AND B.HHR_EFFE_DATE = (SELECT max(B1.HHR_EFFE_DATE) "
                           "FROM hhr_org_dept_b B1 WHERE B1.HHR_SYS_CODE = B.HHR_SYS_CODE AND B1.HHR_DEPT_CODE = B.HHR_DEPT_CODE AND B1.HHR_EFFE_DATE <= :b4 ) "
                           "WHERE A.HHR_SYS_CODE = :b1 AND A.HHR_DEPT_CODE = :b2 AND A.HHR_EFFE_DATE = (SELECT MAX(A1.HHR_EFFE_DATE) FROM hhr_dept_lvl A1 WHERE "
                           " A1.HHR_SYS_CODE = A.HHR_SYS_CODE AND A1.HHR_DEPT_CODE = A.HHR_DEPT_CODE AND A1.HHR_EFFE_DATE <= :b4) "
                           "AND A.HHR_RPT_LVL <> 0 ORDER BY A.HHR_RPT_LVL ASC ")

        rs = conn.execute(sql_catalog, b1=tenant_id, b2=dept_id, b3=lang, b4=prd_end_dt).fetchall()
        for row in rs:
            high_dept_id = row['HHR_DEPT_HIGHER_DEPT']
            high_dept_name = row['HHR_DEPT_NAME']
            if high_dept_name is None:
                high_dept_name = ''
            # 构造 high_dept_dic = { ('2018-01-01','2018-01-31','21004'):[('21001','部门1'),('21002','部门2'),('21003','部门3'),...] , ():[] ,... }
            high_dept_dic[tup_key].append((high_dept_id, high_dept_name))


def get_max_high_dept_count(high_dept_dic):
    """获取最大上级部门数（含自身）"""

    max_h_dept_cnt = 0
    for tup_key, high_dept_lst in high_dept_dic.items():
        h_dept_cnt = len(high_dept_lst)
        if h_dept_cnt > max_h_dept_cnt:
            max_h_dept_cnt = h_dept_cnt
    return max_h_dept_cnt


def get_result_by_cal(run_param, tmpl_line_list, by_trac_detail, py_sum=False, high_dept_dic=None):
    conn = run_param['conn']
    sys_code = run_param['sys_code']
    lang = run_param['lang']
    py_cal_id = run_param['py_cal_id']
    in_emp_id = run_param.get('emp_id', None)
    in_emp_rcd = run_param.get('emp_rcd', None)
    dept_cd = run_param['dept_cd']
    has_child_dept = run_param['has_child_dept']

    field_label_lst = []
    dept_col_num = 0
    result_list = []
    if len(tmpl_line_list) > 0:
        # 获取追溯期间数据
        m_prd_dic = {}
        f_prd_year_num_list = []
        if by_trac_detail:
            m_prd_dic, f_prd_year_num_list = get_prd_year_info_tup(run_param, tmpl_line_list)
        field_label_lst, sql_field_list = get_fld_info_tup(tmpl_line_list, f_prd_year_num_list, by_trac_detail)
        sql_field_string = ','.join(sql_field_list)

        catalog_list = get_fcal_equal_pycal(conn, sys_code, py_cal_id, in_emp_id, in_emp_rcd)
        if catalog_list is not None and len(catalog_list) > 0:
            for row in catalog_list:
                row_line_list = []
                seq_num = row['HHR_SEQ_NUM']
                emp_id = row['HHR_EMPID']
                emp_rcd = row['HHR_EMP_RCD']
                emp_name = row['HHR_NAME']
                prd_bgn_dt = row['HHR_PRD_BGN_DT']
                prd_end_dt = row['HHR_PRD_END_DT']
                py_grp_id = row['HHR_PYGROUP_ID']
                py_cal_id = row['HHR_PY_CAL_ID']
                array_result = get_relt_array_by_seq(conn, sql_field_string, sys_code, lang, seq_num, emp_id, emp_rcd, dept_cd, prd_end_dt, has_child_dept)
                if array_result is None:
                    continue

                # 将薪资明细数据放入list里
                row_line_list.append(emp_id)
                row_line_list.append(emp_name)
                row_line_list.append(emp_rcd)
                row_line_list.append(prd_bgn_dt)
                row_line_list.append(prd_end_dt)
                row_line_list.append(py_grp_id)
                row_line_list.append(py_cal_id)

                field_index = len(row_line_list) - 1
                for item_dic in tmpl_line_list:
                    field_index += 1
                    for field_name, field_info_lst in item_dic.items():
                        py_elem_id = field_info_lst[3]
                        if field_name == 'HHR_TXT001':
                            dept_id = array_result[field_name]
                            dept_id = dept_id if dept_id is not None else ''
                            if array_result['HHR_DEPT_NAME'] is None:
                                dept_desc = ''
                            else:
                                dept_desc = array_result['HHR_DEPT_NAME']

                            if array_result['HHR_DEPT_DETAIL_DESC'] is None:
                                dept_full_desc = ''
                            else:
                                dept_full_desc = array_result['HHR_DEPT_DETAIL_DESC']

                            # 如果是薪资汇总表
                            if py_sum:
                                # 记录部门所在的列
                                dept_col_num = field_index
                                build_upper_lvl_dept_dic(conn, sys_code, lang, prd_bgn_dt, prd_end_dt, dept_id, dept_desc, high_dept_dic)
                            row_line_list.append(dept_id)
                            field_index += 1
                            row_line_list.append(dept_desc)
                            field_index += 1
                            row_line_list.append(dept_full_desc)

                        elif field_name == 'HHR_TXT002':
                            row_line_list.append(array_result[field_name])
                            field_index += 1
                            row_line_list.append(array_result['HHR_POSITION_NAME'])
                        else:
                            row_line_list.append(array_result[field_name])

                        if by_trac_detail:
                            # 如果包含薪资项，则显示追溯列
                            if py_elem_id:
                                for f_prd_year_num in f_prd_year_num_list:
                                    field_index += 1
                                    find_key_tup = (emp_id, emp_rcd, f_prd_year_num)
                                    if find_key_tup in m_prd_dic:
                                        pins_amt_dic = m_prd_dic[find_key_tup]
                                        if py_elem_id in pins_amt_dic:
                                            pin_amt = pins_amt_dic[py_elem_id]
                                            row_line_list.append(pin_amt)
                                        else:
                                            row_line_list.append(None)
                                    else:
                                        row_line_list.append(None)
                result_list.append(row_line_list)
            return field_label_lst, result_list, dept_col_num
    return field_label_lst, result_list, dept_col_num


def get_result_by_date(run_param, tmpl_line_list, by_trac_detail, py_sum=False, high_dept_dic=None):
    conn = run_param['conn']
    user_id = run_param['user_id']
    sys_code = run_param['sys_code']
    lang = run_param['lang']
    py_group_id = run_param['py_group_id']
    start_date = run_param['start_date']
    end_date = run_param['end_date']
    in_emp_id = run_param.get('emp_id', None)
    in_emp_rcd = run_param.get('emp_rcd', None)
    dept_cd = run_param['dept_cd']
    has_child_dept = run_param['has_child_dept']

    field_label_lst = []
    dept_col_num = 0
    result_list = []
    if len(tmpl_line_list) > 0:
        m_prd_dic = {}
        f_prd_year_num_list = []
        if by_trac_detail:
            # 获取所有满足条件的日历
            cal_lst = get_cal_lst_in_date_range(conn, user_id, sys_code, start_date, end_date, in_emp_id, in_emp_rcd, py_group_id)
            # 获取所有日历的追溯期间
            for each_cal_id in cal_lst:
                get_all_periods_by_cal(conn, sys_code, each_cal_id, in_emp_id, in_emp_rcd, f_prd_year_num_list)
                get_cal_retro_data(run_param, each_cal_id, tmpl_line_list, m_prd_dic)

        field_label_lst, sql_field_list = get_fld_info_tup(tmpl_line_list, f_prd_year_num_list, by_trac_detail)
        sql_field_string = ','.join(sql_field_list)

        dept_col_num = 0
        result_dic = {}
        catalog_list = get_fcal_equal_pycal_withoutcal(conn, user_id, sys_code, start_date, end_date, in_emp_id, in_emp_rcd, py_group_id)
        if catalog_list is not None and len(catalog_list) > 0:
            for row in catalog_list:
                row_line_list = []
                seq_num = row['hhr_seq_num']
                emp_id = row['hhr_empid']
                emp_name = row['hhr_name']
                emp_rcd = row['hhr_emp_rcd']
                prd_bgn_dt = row['hhr_prd_bgn_dt']
                prd_end_dt = row['hhr_prd_end_dt']
                py_grp_id = row['hhr_pygroup_id']
                py_cal_id = row['hhr_py_cal_id']
                array_result = get_relt_array_by_seq(conn, sql_field_string, sys_code, lang, seq_num, emp_id, emp_rcd, dept_cd, prd_end_dt, has_child_dept)
                if array_result is None:
                    continue
    
                # 将薪资明细数据放入list里
                row_line_list.append(emp_id)
                row_line_list.append(emp_name)
                row_line_list.append(emp_rcd)
                row_line_list.append(prd_bgn_dt)
                row_line_list.append(prd_end_dt)
                row_line_list.append(py_grp_id)
                # 不输入日历参数时，日历列留空
                row_line_list.append(None)
    
                result_key = (emp_id, emp_rcd, prd_bgn_dt, prd_end_dt)
                temp_list = []
                key_exists = False
                if result_key in result_dic:
                    temp_list = result_dic[result_key]
                    key_exists = True
    
                field_index = len(row_line_list) - 1
                i = len(row_line_list) - 1
                for item_dic in tmpl_line_list:
                    field_index += 1
                    i += 1
                    for field_name, field_info_lst in item_dic.items():
                        py_elem_id = field_info_lst[3]
                        seq_btwn = field_info_lst[4]
                        if key_exists:
                            if seq_btwn == 'SUM':
                                if array_result[field_name] is None:
                                    temp_list[i] = temp_list[i]
                                elif temp_list[i] is None:
                                    temp_list[i] = array_result[field_name]
                                else:
                                    temp_list[i] = temp_list[i] + array_result[field_name]
                            elif seq_btwn == 'MAX':
                                temp_list[i] = array_result[field_name]
                                if field_name == 'HHR_TXT001':
                                    i += 1
                                    if array_result['HHR_DEPT_NAME'] is None:
                                        temp_list[i] = ''
                                    else:
                                        temp_list[i] = array_result['HHR_DEPT_NAME']

                                    i += 1
                                    if array_result['HHR_DEPT_DETAIL_DESC'] is None:
                                        temp_list[i] = ''
                                    else:
                                        temp_list[i] = array_result['HHR_DEPT_DETAIL_DESC']

                                elif field_name == 'HHR_TXT002':
                                    i += 1
                                    if array_result['HHR_POSITION_NAME'] is None:
                                        temp_list[i] = ''
                                    else:
                                        temp_list[i] = array_result['HHR_POSITION_NAME']
                            else:
                                i += 1
                        else:
                            if field_name == 'HHR_TXT001':
                                # 如果是薪资汇总表
                                dept_id = array_result[field_name]
                                dept_id = dept_id if dept_id is not None else ''
                                if array_result['HHR_DEPT_NAME'] is None:
                                    dept_desc = ''
                                else:
                                    dept_desc = array_result['HHR_DEPT_NAME']

                                if array_result['HHR_DEPT_DETAIL_DESC'] is None:
                                    dept_full_desc = ''
                                else:
                                    dept_full_desc = array_result['HHR_DEPT_DETAIL_DESC']

                                if py_sum:
                                    dept_col_num = field_index
                                    build_upper_lvl_dept_dic(conn, sys_code, lang, prd_bgn_dt, prd_end_dt, dept_id, dept_desc, high_dept_dic)
                                row_line_list.append(dept_id)
                                field_index += 1
                                row_line_list.append(dept_desc)
                                field_index += 1
                                row_line_list.append(dept_full_desc)
                            elif field_name == 'HHR_TXT002':
                                row_line_list.append(array_result[field_name])
                                field_index += 1
                                row_line_list.append(array_result['HHR_POSITION_NAME'])
                            else:
                                if array_result[field_name] is None:
                                    row_line_list.append(0)
                                else:
                                    row_line_list.append(array_result[field_name])
    
                        if by_trac_detail:
                            # 如果包含薪资项，则显示追溯列
                            if py_elem_id:
                                for f_prd_year_num in f_prd_year_num_list:
                                    field_index += 1
                                    i += 1
                                    find_key_tup = (emp_id, emp_rcd, py_cal_id, f_prd_year_num)
                                    if find_key_tup in m_prd_dic:
                                        pins_amt_dic = m_prd_dic[find_key_tup]
                                        if py_elem_id in pins_amt_dic:
                                            pin_amt = pins_amt_dic[py_elem_id]
                                            # 在多个相同期间内对每个追溯列进行求和
                                            if key_exists:
                                                if temp_list[i] is None:
                                                    temp_list[i] = 0
                                                temp_list[i] = temp_list[i] + pin_amt
                                            else:
                                                row_line_list.append(pin_amt)
                                        else:
                                            if key_exists:
                                                temp_list[i] = temp_list[i]
                                            else:
                                                row_line_list.append(None)
                                    else:
                                        if key_exists:
                                                temp_list[i] = temp_list[i]
                                        else:
                                            row_line_list.append(None)
    
                if result_key not in result_dic:
                    result_dic[result_key] = copy(row_line_list)
                else:
                    result_dic[result_key] = temp_list
    
            result_list = result_dic.values()
            return field_label_lst, result_list, dept_col_num

    return field_label_lst, result_list, dept_col_num


def run_py_dtl_by_cal(run_param, tmpl_line_list, by_trac_detail):
    field_label_lst, result_list, dept_col_num = get_result_by_cal(run_param, tmpl_line_list, by_trac_detail)
    # 生成Excel
    create_py_detail_excl(field_label_lst, result_list, run_param)


def run_py_dtl_by_date(run_param, tmpl_line_list, by_trac_detail):
    field_label_lst, result_list, dept_col_num = get_result_by_date(run_param, tmpl_line_list, by_trac_detail)
    # 生成Excel
    create_py_detail_excl(field_label_lst, result_list, run_param)


def run_py_sum(run_param, tmpl_line_list, by_trac_detail, by_what):
    tree_lvl = run_param['tree_lvl']

    header_inx_lst = []
    high_dept_dic = {}
    field_label_lst = None
    result_list = None
    dept_col_num = 0
    if by_what == 'C':
        field_label_lst, result_list, dept_col_num = get_result_by_cal(run_param, tmpl_line_list, by_trac_detail, py_sum=True, high_dept_dic=high_dept_dic)
    elif by_what == 'D':
        field_label_lst, result_list, dept_col_num = get_result_by_date(run_param, tmpl_line_list, by_trac_detail, py_sum=True, high_dept_dic=high_dept_dic)
    max_h_dept_cnt = get_max_high_dept_count(high_dept_dic)
    if result_list is not None:
        sum_result_dic = {}
        for r_line in result_list:
            start_dt = r_line[3]
            end_dt = r_line[4]
            dept_id = r_line[dept_col_num]
            dept_id = dept_id if dept_id is not None else ''
            tup_key = (start_dt, end_dt, dept_id)

            if tup_key not in sum_result_dic:
                sum_result_dic[tup_key] = []
                temp_lst = r_line[7:]
                # 在列表开头插入开始日期和结束日期
                temp_lst.insert(0, end_dt)
                temp_lst.insert(0, start_dt)
                sum_result_dic[tup_key] = temp_lst
            else:
                new_lst = r_line[7:]
                new_lst.insert(0, end_dt)
                new_lst.insert(0, start_dt)
                old_lst = sum_result_dic[tup_key]
                # 从第三列开始处理，前两列分别为开始日期、结束日期
                for i in range(2, len(old_lst)):
                    # 如果当前列是部门或者部门描述列，则不处理，(dept_col_num - 7) + 2为部门编码在新列表中的索引
                    if (i == (dept_col_num - 7) + 2) or (i == (dept_col_num - 7) + 2 + 1):
                        pass
                    else:
                        if old_lst[i] is None:
                            old_lst[i] = 0
                        if new_lst[i] is None:
                            new_lst[i] = 0
                        old_lst[i] += new_lst[i]

        # sum_result_dic = sorted(sum_result_dic.keys(),key=lambda x: (x[0],x[0],x[2]))
        sorted_sum_key_lst = sorted(sum_result_dic.keys())

        new_sum_rs_dic = {}
        for tup_key in sorted_sum_key_lst:
            val_lst = sum_result_dic[tup_key]
            # 获取上级部门列表
            high_dept_tup_lst = high_dept_dic[tup_key]
            # 获取当前部门上级层级数量
            h_dept_cnt = len(high_dept_tup_lst)
            for j in range(max_h_dept_cnt):
                if j + 1 > h_dept_cnt:
                    val_lst.insert(2 + h_dept_cnt, '')
                else:
                    if tree_lvl:
                        dept_id_desc = str(high_dept_tup_lst[j][0]) + '#-#' + str(high_dept_tup_lst[j][1])
                        val_lst.insert(2, dept_id_desc)
                    else:
                        # 将每一个上级部门的描述按由高到低的顺序插入到结束日期之后
                        dept_desc = high_dept_tup_lst[j][1]
                        val_lst.insert(2, dept_desc)
            new_sum_rs_dic[tup_key] = val_lst
        result_list = new_sum_rs_dic.values()

        # 如果选择按树层级汇总
        if tree_lvl:
            if result_list is not None:
                tree_lvl = int(tree_lvl)
                # 如果树层级超过了最大上级部门树上限
                if tree_lvl > max_h_dept_cnt:
                    tree_lvl = max_h_dept_cnt

                # 获取部门ID当前所在的列
                cur_dept_col_num = (dept_col_num - 7) + 2 + max_h_dept_cnt
                result_dic = {}
                for row_line in result_list:
                    # 记录所选取列的索引
                    header_inx_lst = []
                    start_dt = row_line[0]
                    end_dt = row_line[1]
                    header_inx_lst.append(0)
                    header_inx_lst.append(1)

                    sum_key_lst = [start_dt, end_dt]
                    new_line = [start_dt, end_dt]
                    for lvl_inx in range(2, tree_lvl + 2):
                        dept_id_desc = row_line[lvl_inx]
                        temp_lst = dept_id_desc.split('#-#')
                        dept_id = temp_lst[0]
                        if len(temp_lst) > 1:
                            dept_desc = temp_lst[1]
                        else:
                            dept_desc = ''

                        sum_key_lst.append(dept_id)
                        new_line.append(dept_desc)
                        header_inx_lst.append(lvl_inx)

                    k = 0
                    for col_val in row_line:
                        k += 1
                        if k > (2 + max_h_dept_cnt):
                            # 跳过部门ID和部门描述列
                            if (k - 1) != cur_dept_col_num and (k - 1) != (cur_dept_col_num + 1):
                                new_line.append(col_val)
                                header_inx_lst.append(k-1)

                    sum_key = tuple(sum_key_lst)
                    if sum_key not in result_dic:
                        result_dic[sum_key] = new_line
                    else:
                        old_line = result_dic[sum_key]
                        start_indx = len(sum_key)
                        for x in range(start_indx, len(old_line)):
                            old_line[x] += new_line[x]
                result_list = result_dic.values()
    # 生成Excel
    create_py_sum_excl(max_h_dept_cnt, field_label_lst, result_list, header_inx_lst, run_param)
