# -*- coding: utf-8 -*-

from sqlalchemy.sql import text

from .excelutils.create_excel import create_pers_trans_excl


def get_high_dept(conn, tenant_id, dept_cd, lang, effe_dt):
    """
    Desc: 查询部门的上级部门
    Author: 陶雨
    Date: 2019/01/26
    """

    sql_catalog = "select a.hhr_parent_dept, b.hhr_dept_name from boogoo_corehr.hhr_org_dept_lvl a " \
                  "left join boogoo_corehr.hhr_org_dept b on b.tenant_id = a.tenant_id and b.hhr_dept_code = a.hhr_parent_dept " \
                  "and b.hhr_efft_date = (select max(b1.hhr_efft_date) from boogoo_corehr.hhr_org_dept b1 " \
                  "where b1.tenant_id = b.tenant_id and b1.hhr_dept_code = b.hhr_dept_code and b1.hhr_efft_date <= :b4 ) " \
                  "left join boogoo_corehr.hhr_org_dept_tl bl on bl.id = b.id and bl.lang = :b3 " \
                  "where a.tenant_id = :b1 and a.hhr_dept_code = :b2 and a.hhr_efft_date = (select max(a1.hhr_efft_date) " \
                  "from boogoo_corehr.hhr_org_dept_lvl a1 where a1.tenant_id = a.tenant_id and a1.hhr_dept_code = a.hhr_dept_code " \
                  "and a1.hhr_efft_date <= :b4) and a.hhr_rpt_lvl <> 0 order by a.hhr_rpt_lvl desc "

    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=dept_cd, b3=lang, b4=effe_dt).fetchall()

    return rs


def get_dept(conn, user_id, tenant_id, dept_cd, end_date, start_date, has_child_dept):
    """
    Desc: 查询部门
    Author: 陶雨
    Date: 2019/04/23
    """

    sql_catalog1 = "select B.hhr_dept_code, B.hhr_dept_name, B.hhr_dept_schema_level, B.hhr_dept_schema_left, B.hhr_dept_schema_right " \
                   "from boogoo_corehr.hhr_org_dept_schema b where b.tenant_id = :b1 and b.hhr_status = 'Y' " \
                   "and b.hhr_efft_date = (select max(b1.hhr_efft_date) from boogoo_corehr.hhr_org_tree b1	 " \
                   "where b1.tenant_id = b.tenant_id " \
                   "and b1.hhr_tree_code = 'ORG-DEPT-TREE' and b1.hhr_efft_date <= :b3 ) "

    sql_catalog2 = "and b.hhr_dept_code = :b2 "

    sql_catalog3 = "and ( b.hhr_dept_code = :b2 or b.hhr_dept_code in " \
                   "(select a1.hhr_dept_code from boogoo_corehr.hhr_org_dept_lvl a1 where a1.tenant_id = :b1 and a1.hhr_parent_dept = :b2 " \
                   "and a1.hhr_efft_date = (select max(a2.hhr_efft_date) from boogoo_corehr.hhr_org_dept_lvl a2 where a2.tenant_id = a1.tenant_id " \
                   "and a2.hhr_dept_code = a1.hhr_dept_code and a2.hhr_efft_date <= :b3 ))) "

    sql_catalog4 = "order by b.hhr_dept_schema_left, B.hhr_dept_code "

    # 如果传递了部门参数
    if dept_cd:
        # 如果包含子部门
        if has_child_dept == 'Y':
            sql_catalog = sql_catalog1 + sql_catalog3 + sql_catalog4
        else:
            sql_catalog = sql_catalog1 + sql_catalog2 + sql_catalog4
    else:
        sql_catalog = sql_catalog1 + sql_catalog4

    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=dept_cd, b3=end_date, b4=start_date).fetchall()

    return rs


def get_all_emp_count(conn, user_id, tenant_id, dept_cd, start_date, end_date, has_child_dept):
    """
    Desc: 查询总员工
    Author: 陶雨
    Date: 2019/04/23
    """

    sql_catalog1 = "select count(b.hhr_empid) " \
                   "from boogoo_corehr.hhr_org_per_jobdata b where b.tenant_id = :b1 and b.hhr_efft_date = (select max(b1.hhr_efft_date) " \
                   "from boogoo_corehr.hhr_org_per_jobdata b1 where b1.hhr_empid = b.hhr_empid and b1.hhr_emp_rcd = b.hhr_emp_rcd " \
                   "and b1.tenant_id = b.tenant_id and b1.hhr_efft_date <= :b3 ) " \
                   "and b.hhr_efft_seq = (select max(b2.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata b2 " \
                   "where b2.hhr_empid = b.hhr_empid and b2.hhr_emp_rcd = b.hhr_emp_rcd	and " \
                   "b2.tenant_id = b.tenant_id and b2.hhr_efft_date = b.hhr_efft_date) and b.hhr_status = 'Y' "

    sql_catalog2 = "and b.hhr_dept_code = :b2 "

    sql_catalog3 = "and (b.hhr_dept_code = :b2 or b.hhr_dept_code in (select a1.hhr_dept_code from boogoo_corehr.hhr_org_dept_lvl a1 " \
                   "where a1.tenant_id = :b1 and a1.hhr_parent_dept = :b2 " \
                   "and a1.hhr_efft_date = (select max(a2.hhr_efft_date) from boogoo_corehr.hhr_org_dept_lvl a2 " \
                   "where a2.tenant_id = a1.tenant_id and a2.hhr_dept_code = a1.hhr_dept_code " \
                   "and a2.hhr_efft_date <= :b3))) "

    sql_catalog4 = "and b.hhr_job_indicator = 'P' "

    # 如果包含子部门
    if has_child_dept == 'Y':
        sql_catalog = sql_catalog1 + sql_catalog3 + sql_catalog4
    else:
        sql_catalog = sql_catalog1 + sql_catalog2 + sql_catalog4

    row = conn.execute(text(sql_catalog), b1=tenant_id, b2=dept_cd, b3=end_date, b4=start_date).fetchone()

    return row


def get_emp_info(conn, user_id, tenant_id, dept_cd, start_date, end_date, has_child_dept, action_cd, company, hr_sub_range, emp_class, emp_sub_class, emp_status):
    """
    Desc: 查询员工
    Author: 陶雨
    Date: 2019/04/23
    """

    sql_catalog1 = "select count(b.hhr_empid) " \
                   "from boogoo_corehr.hhr_org_per_jobdata b where b.tenant_id = :b1 and b.hhr_efft_date = (select max(b1.hhr_efft_date) " \
                   "from boogoo_corehr.hhr_org_per_jobdata b1 where b1.hhr_empid = b.hhr_empid and b1.hhr_emp_rcd = b.hhr_emp_rcd " \
                   "and b1.hhr_status = b.hhr_status and b1.hhr_action_type_code = b.hhr_action_type_code " \
                   "and b1.tenant_id = b.tenant_id and b1.hhr_efft_date <= :b3 and b1.hhr_efft_date >= :b4 ) " \
                   "and b.hhr_status = :b10 "

    sql_catalog2 = "and b.hhr_dept_code = :b2 "

    sql_catalog3 = "and (b.hhr_dept_code = :b2 or b.hhr_dept_code in (select a1.hhr_dept_code from boogoo_corehr.hhr_org_dept_lvl a1 " \
                   "where a1.tenant_id = :b1 and a1.hhr_parent_dept = :b2 " \
                   "and a1.hhr_efft_date = (select max(a2.hhr_efft_date) from boogoo_corehr.hhr_org_dept_lvl a2 " \
                   "where a2.tenant_id = a1.tenant_id and a2.hhr_dept_code = a1.hhr_dept_code " \
                   "and a2.hhr_efft_date <= :b3))) "

    sql_catalog4 = "and b.hhr_action_type_code = :b5 "

    sql_catalog5 = "and b.hhr_company_code = :b6 "

    sql_catalog6 = "and b.hhr_org_per_jobdata_attr01 = :b7 "

    sql_catalog7 = "and b.hhr_emp_class = :b8 "

    sql_catalog8 = "and b.hhr_sub_emp_class = :b9 "

    # 如果包含子部门
    if has_child_dept == 'Y':
        sql_catalog = sql_catalog1 + sql_catalog3
        if action_cd:
            sql_catalog = sql_catalog + sql_catalog4
        if company:
            sql_catalog = sql_catalog + sql_catalog5
        if hr_sub_range:
            sql_catalog = sql_catalog + sql_catalog6
        if emp_class:
            sql_catalog = sql_catalog + sql_catalog7
        if emp_sub_class:
            sql_catalog = sql_catalog + sql_catalog8
    else:
        sql_catalog = sql_catalog1 + sql_catalog2
        if action_cd:
            sql_catalog = sql_catalog + sql_catalog4
        if company:
            sql_catalog = sql_catalog + sql_catalog5
        if hr_sub_range:
            sql_catalog = sql_catalog + sql_catalog6
        if emp_class:
            sql_catalog = sql_catalog + sql_catalog7
        if emp_sub_class:
            sql_catalog = sql_catalog + sql_catalog8

    row = conn.execute(text(sql_catalog), b1=tenant_id, b2=dept_cd, b3=end_date, b4=start_date, b5=action_cd, b6=company, b7=hr_sub_range, b8=emp_class, b9=emp_sub_class, b10=emp_status).fetchone()

    return row


def get_emp_trans_info(conn, user_id, tenant_id, dept_cd, start_date, end_date, has_child_dept, action_cd, company, hr_sub_range, emp_class, emp_sub_class):
    """
    Desc: 查询调入/调出员工
    Author: 陶雨
    Date: 2019/04/23
    """

    sql_catalog1 = "select b.tenant_id, b.hhr_empid, b.hhr_dept_code, b.hhr_efft_date, b.hhr_emp_rcd, b.hhr_efft_seq, b.hhr_action_type_code " \
                   "from boogoo_corehr.hhr_org_per_jobdata b where b.tenant_id = :b1 and b.hhr_efft_date = (select max(b1.hhr_efft_date) " \
                   "from boogoo_corehr.hhr_org_per_jobdata b1 where b1.hhr_empid = b.hhr_empid and b1.hhr_emp_rcd = b.hhr_emp_rcd " \
                   "and b1.hhr_status = b.hhr_status and b1.hhr_action_type_code = b.hhr_action_type_code " \
                   "and b1.tenant_id = b.tenant_id and b1.hhr_efft_date <= :b3 and b1.hhr_efft_date >= :b4 ) " \
                   "and b.hhr_efft_seq = (select max(b2.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata b2 " \
                   "where b2.hhr_empid = b.hhr_empid and b2.hhr_emp_rcd = b.hhr_emp_rcd and " \
                   "b2.tenant_id = b.tenant_id and b2.hhr_efft_date = b.hhr_efft_date) and b.hhr_status = 'Y' "

    sql_catalog2 = "and b.hhr_dept_code = :b2 "

    sql_catalog3 = "and (b.hhr_dept_code = :b2 or b.hhr_dept_code in (select a1.hhr_dept_code from boogoo_corehr.hhr_org_dept_lvl a1 " \
                   "where a1.tenant_id = :b1 and a1.hhr_parent_dept = :b2 " \
                   "and a1.hhr_efft_date = (select max(a2.hhr_efft_date) from boogoo_corehr.hhr_org_dept_lvl a2 " \
                   "where a2.tenant_id = a1.tenant_id and a2.hhr_dept_code = a1.hhr_dept_code " \
                   "and a2.hhr_efft_date <= :b3))) "

    sql_catalog4 = "and b.hhr_action_type_code = :b5 "

    sql_catalog5 = "and b.hhr_company_code = :b6 "

    sql_catalog6 = "and b.hhr_org_per_jobdata_attr01 = :b7 "

    sql_catalog7 = "and b.hhr_emp_class = :b8 "

    sql_catalog8 = "and b.hhr_sub_emp_class = :b9 "

    # 如果包含子部门
    if has_child_dept == 'Y':
        sql_catalog = sql_catalog1 + sql_catalog3
        if action_cd:
            sql_catalog = sql_catalog + sql_catalog4
        if company:
            sql_catalog = sql_catalog + sql_catalog5
        if hr_sub_range:
            sql_catalog = sql_catalog + sql_catalog6
        if emp_class:
            sql_catalog = sql_catalog + sql_catalog7
        if emp_sub_class:
            sql_catalog = sql_catalog + sql_catalog8
    else:
        sql_catalog = sql_catalog1 + sql_catalog2
        if action_cd:
            sql_catalog = sql_catalog + sql_catalog4
        if company:
            sql_catalog = sql_catalog + sql_catalog5
        if hr_sub_range:
            sql_catalog = sql_catalog + sql_catalog6
        if emp_class:
            sql_catalog = sql_catalog + sql_catalog7
        if emp_sub_class:
            sql_catalog = sql_catalog + sql_catalog8

    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=dept_cd, b3=end_date, b4=start_date, b5=action_cd, b6=company, b7=hr_sub_range, b8=emp_class, b9=emp_sub_class).fetchall()

    return rs


def get_emp_untrans_info(conn, user_id, tenant_id, dept_cd, start_date, end_date, has_child_dept, action_cd, company, hr_sub_range, emp_class, emp_sub_class):
    """
    Desc: 查询非调动员工
    Author: 陶雨
    Date: 2019/05/06
    """

    sql_catalog1 = "select b.tenant_id, b.hhr_empid, b.hhr_dept_code, b.hhr_efft_date, b.hhr_emp_rcd, b.hhr_efft_seq, b.hhr_action_type_code " \
                   "from boogoo_corehr.hhr_org_per_jobdata b where b.tenant_id = :b1 and b.hhr_efft_date = (select max(b1.hhr_efft_date) " \
                   "from boogoo_corehr.hhr_org_per_jobdata b1 where b1.hhr_empid = b.hhr_empid and b1.hhr_emp_rcd = b.hhr_emp_rcd " \
                   "and b1.hhr_status = b.hhr_status and b1.hhr_action_type_code = b.hhr_action_type_code " \
                   "and b1.tenant_id = b.tenant_id and b1.hhr_efft_date <= :b3 ) " \
                   "and b.hhr_efft_seq = (select max(b2.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata b2 " \
                   "where b2.hhr_empid = b.hhr_empid and b2.hhr_emp_rcd = b.hhr_emp_rcd	and " \
                   "b2.tenant_id = b.tenant_id and b2.hhr_efft_date = b.hhr_efft_date) and b.hhr_status = 'Y' "

    sql_catalog2 = "and b.hhr_dept_code = :b2 "

    sql_catalog3 = "and (b.hhr_dept_code = :b2 or b.hhr_dept_code in (select a1.hhr_dept_code from boogoo_corehr.hhr_org_dept_lvl a1 " \
                   "where a1.tenant_id = :b1 and a1.hhr_parent_dept = :b2 " \
                   "and a1.hhr_efft_date = (select max(a2.hhr_efft_date) from boogoo_corehr.hhr_org_dept_lvl a2 " \
                   "where a2.tenant_id = a1.tenant_id and a2.hhr_dept_code = a1.hhr_dept_code " \
                   "and a2.hhr_efft_date <= :b3))) "

    sql_catalog4 = "AND B.hhr_action_type_code <> '' "

    sql_catalog5 = "and b.hhr_company_code = :b6 "

    sql_catalog6 = "AND B.hhr_org_per_jobdata_attr01 = :b7 "

    sql_catalog7 = "and b.hhr_emp_class = :b8 "

    sql_catalog8 = "and b.hhr_sub_emp_class = :b9 "

    # 如果包含子部门
    if has_child_dept == 'Y':
        sql_catalog = sql_catalog1 + sql_catalog3
        if action_cd:
            sql_catalog = sql_catalog + sql_catalog4
        if company:
            sql_catalog = sql_catalog + sql_catalog5
        if hr_sub_range:
            sql_catalog = sql_catalog + sql_catalog6
        if emp_class:
            sql_catalog = sql_catalog + sql_catalog7
        if emp_sub_class:
            sql_catalog = sql_catalog + sql_catalog8
    else:
        sql_catalog = sql_catalog1 + sql_catalog2
        if action_cd:
            sql_catalog = sql_catalog + sql_catalog4
        if company:
            sql_catalog = sql_catalog + sql_catalog5
        if hr_sub_range:
            sql_catalog = sql_catalog + sql_catalog6
        if emp_class:
            sql_catalog = sql_catalog + sql_catalog7
        if emp_sub_class:
            sql_catalog = sql_catalog + sql_catalog8

    rs = conn.execute(text(sql_catalog), b1=tenant_id, b2=dept_cd, b3=end_date, b4=start_date, b5=action_cd, b6=company, b7=hr_sub_range, b8=emp_class, b9=emp_sub_class).fetchall()

    return rs


def get_enter_per_info(conn, tenant_id, emp_id, emp_rcd, start_date, emp_effe_date):
    """
    Desc: 查询调入人员相邻数据
    Author: 陶雨
    Date: 2019/04/23
    """

    enter_dept_cd = None

    s1 = text("select b.hhr_dept_code from boogoo_corehr.hhr_org_per_jobdata b where b.tenant_id = :b1 "
              "and b.hhr_empid = :b2 and b.hhr_emp_rcd = :b3 and b.hhr_efft_date = (select max(b1.hhr_efft_date) "
              "from boogoo_corehr.hhr_org_per_jobdata b1	where b1.hhr_empid = b.hhr_empid and b1.hhr_emp_rcd = b.hhr_emp_rcd "
              "and b1.tenant_id = b.tenant_id and b1.hhr_efft_date < :b5 ) "
              "and b.hhr_efft_seq = (select max(b2.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata b2 "
              "where b2.hhr_empid = b.hhr_empid and b2.hhr_emp_rcd = b.hhr_emp_rcd and b2.tenant_id = b.tenant_id	 "
              "and b2.hhr_efft_date = b.hhr_efft_date)")
    row = conn.execute(s1, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=start_date, b5=emp_effe_date).fetchone()
    if row is not None:
        enter_dept_cd = row['HHR_DEPT_CODE']

    return enter_dept_cd


def get_out_per_info(conn, tenant_id, emp_id, emp_rcd, end_date, start_date, dept_code, emp_effe_date):
    """
    Desc: 查询调出人员相邻数据
    Author: 陶雨
    Date: 2019/04/23
    """

    out_dept_cd = None
    out_effe_date = None

    s1 = text("select b.hhr_dept_code,b.hhr_efft_date from boogoo_corehr.hhr_org_per_jobdata b where b.tenant_id = :b1 "
              "and b.hhr_dept_code <> :b6 and b.hhr_action_type_code = 'z8' "
              "and b.hhr_empid = :b2 and b.hhr_emp_rcd = :b3 and b.hhr_efft_date = (select min(b1.hhr_efft_date) "
              "from boogoo_corehr.hhr_org_per_jobdata b1	where b1.hhr_empid = b.hhr_empid and b1.hhr_emp_rcd = b.hhr_emp_rcd "
              "and b1.tenant_id = b.tenant_id and b1.hhr_efft_date > :b7 and b1.hhr_efft_date <= :b4 ) "
              "and b.hhr_efft_seq = (select max(b2.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata b2 "
              "where b2.hhr_empid = b.hhr_empid and b2.hhr_emp_rcd = b.hhr_emp_rcd and b2.tenant_id = b.tenant_id	 "
              "and b2.hhr_efft_date = b.hhr_efft_date)")
    row = conn.execute(s1, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=end_date, b5=start_date, b6=dept_code, b7=emp_effe_date).fetchone()
    if row is not None:
        out_dept_cd = row['HHR_DEPT_CODE']
        out_effe_date = row['HHR_EFFt_DATE']

    return out_dept_cd,out_effe_date


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



def run_per_trans(run_param):
    conn = run_param['conn']
    user_id = run_param['user_id']
    tenant_id = run_param['tenant_id']
    lang = run_param['lang']
    start_date = run_param['start_date']
    end_date = run_param['end_date']
    dept_cd = run_param['dept_cd']
    has_child_dept = run_param['has_child_dept']
    company = run_param['company']
    hr_sub_range = run_param['hr_sub_range']
    emp_class = run_param['emp_class']
    emp_sub_class = run_param['emp_sub_class']

    result_list = []

    # 获取部门
    dept_lst = get_dept(conn, user_id, tenant_id, dept_cd, end_date, start_date, has_child_dept)
    for dept_row in dept_lst:
        row_list = []
        in_dept_cd = dept_row['hhr_dept_code']
        # 查询部门的上级部门
        high_dept_lst = get_high_dept(conn, tenant_id, in_dept_cd, lang, end_date)
        if dept_row['hhr_dept_schema_level'] == '6':
            for high_dept in high_dept_lst:
                row_list.append(high_dept['hhr_dept_name'])
            row_list.append(dept_row['hhr_dept_name'])
        if dept_row['hhr_dept_schema_level'] == '5':
            for high_dept in high_dept_lst:
                row_list.append(high_dept['hhr_dept_name'])
            row_list.append(dept_row['hhr_dept_name'])
            row_list.append('')
        if dept_row['hhr_dept_schema_level'] == '4':
            for high_dept in high_dept_lst:
                row_list.append(high_dept['hhr_dept_name'])
            row_list.append(dept_row['hhr_dept_name'])
            row_list.append('')
            row_list.append('')
        if dept_row['hhr_dept_schema_level'] == '3':
            for high_dept in high_dept_lst:
                row_list.append(high_dept['hhr_dept_name'])
            row_list.append(dept_row['hhr_dept_name'])
            row_list.append('')
            row_list.append('')
            row_list.append('')
        if dept_row['hhr_dept_schema_level'] == '2':
            for high_dept in high_dept_lst:
                row_list.append(high_dept['hhr_dept_name'])
            row_list.append(dept_row['hhr_dept_name'])
            row_list.append('')
            row_list.append('')
            row_list.append('')
            row_list.append('')
        if dept_row['hhr_dept_schema_level'] == '1':
            row_list.append(dept_row['hhr_dept_name'])
            row_list.append('')
            row_list.append('')
            row_list.append('')
            row_list.append('')
            row_list.append('')

        # 总人数
        count_row = get_all_emp_count(conn, user_id, tenant_id, in_dept_cd, start_date, end_date, has_child_dept)
        if count_row is not None:
            total_count = count_row[0]
        else:
            total_count = 0
        row_list.append(total_count)
        # 晋升人数
        action_cd = 'Z6'
        emp_status = 'Y'
        count_row = get_emp_info(conn, user_id, tenant_id, in_dept_cd, start_date, end_date, has_child_dept,
                                   action_cd, company, hr_sub_range, emp_class, emp_sub_class, emp_status)
        if count_row is not None:
            total_count = count_row[0]
        else:
            total_count = 0
        row_list.append(total_count)
        # 降级人数
        action_cd = 'Z7'
        emp_status = 'Y'
        count_row = get_emp_info(conn, user_id, tenant_id, in_dept_cd, start_date, end_date, has_child_dept,
                                   action_cd, company, hr_sub_range, emp_class, emp_sub_class, emp_status)
        if count_row is not None:
            total_count = count_row[0]
        else:
            total_count = 0
        row_list.append(total_count)
        # 入职人数
        action_cd = 'HIR'
        emp_status = 'Y'
        count_row1 = get_emp_info(conn, user_id, tenant_id, in_dept_cd, start_date, end_date, has_child_dept,
                                    action_cd, company, hr_sub_range, emp_class, emp_sub_class, emp_status)
        if count_row1 is not None:
            total_hir_count = count_row1[0]
        else:
            total_hir_count = 0

        action_cd = 'Z2'
        emp_status = 'Y'
        count_row2 = get_emp_info(conn, user_id, tenant_id, in_dept_cd, start_date, end_date, has_child_dept,
                                    action_cd, company, hr_sub_range, emp_class, emp_sub_class, emp_status)

        if count_row2 is not None:
            total_z2_count = count_row2[0]
        else:
            total_z2_count = 0
        row_list.append(total_hir_count + total_z2_count)
        # 离职人数
        action_cd = 'Z9'
        emp_status = 'N'
        count_row = get_emp_info(conn, user_id, tenant_id, in_dept_cd, start_date, end_date, has_child_dept,
                                    action_cd, company, hr_sub_range, emp_class, emp_sub_class, emp_status)
        if count_row is not None:
            total_z9_count = count_row[0]
        else:
            total_z9_count = 0
        row_list.append(total_z9_count)
        # 调入/调出人数
        action_cd = 'Z8'
        enter_lst = []
        out_lst = []
        trans_per_lst = get_emp_trans_info(conn, user_id, tenant_id, in_dept_cd, start_date, end_date, has_child_dept,
                                   action_cd, company, hr_sub_range, emp_class, emp_sub_class)
        for trans_per in trans_per_lst:
            emp_id = trans_per['hhr_empid']
            emp_rcd = trans_per['hhr_emp_rcd']
            emp_effe_date = trans_per['hhr_efft_date']
            tr_dept_code = trans_per['hhr_dept_code']
            enter_dept_cd = get_enter_per_info(conn, tenant_id, emp_id, emp_rcd, end_date, emp_effe_date)
            if enter_dept_cd is not None:
                if enter_dept_cd != tr_dept_code:
                    enter_lst.append(trans_per)
        untrans_per_lst = get_emp_untrans_info(conn, user_id, tenant_id, in_dept_cd, start_date, end_date, has_child_dept,
                                           action_cd, company, hr_sub_range, emp_class, emp_sub_class)
        for untrans_per in untrans_per_lst:
            emp_id = untrans_per['hhr_empid']
            emp_rcd = untrans_per['hhr_emp_rcd']
            emp_effe_date = untrans_per['hhr_efft_date']
            un_dept_code = untrans_per['hhr_dept_code']
            out_dept_cd, out_effe_date = get_out_per_info(conn, tenant_id, emp_id, emp_rcd, end_date, start_date, un_dept_code, emp_effe_date)
            if out_dept_cd is not None:
                if out_effe_date.strftime("%Y-%m-%d") >= start_date.strftime("%Y-%m-%d"):
                    out_lst.append(untrans_per)
        row_list.append(len(enter_lst))
        row_list.append(len(out_lst))
        # 增加人数
        row_list.append(total_hir_count + total_z2_count + len(enter_lst))
        # 减少人数
        row_list.append(total_z9_count + len(out_lst))

        result_list.append(row_list)

    create_pers_trans_excl(result_list, run_param)
