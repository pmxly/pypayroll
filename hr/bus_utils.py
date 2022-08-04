# -*- coding: utf-8 -*-

from ..dbengine import DataBaseAlchemy
from sqlalchemy.sql import text
from ..utils import get_current_date, get_current_dttm
from copy import deepcopy
from .employee import EmployeeRecord
from sqlalchemy.exc import OperationalError
import logging

posn_emp_dic = {}
emp_dic = {}
emp_rpt_chk_stmt = text(
    "select 'Y' from boogoo_corehr.hhr_org_emp_rpt_lvl where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
    "and hhr_report_type = :b4 and hhr_report_position = :b5 and hhr_rpt_emp_id = :b6 and hhr_rpt_emp_rcd = :b7 ")

emp_rtp_ins_stmt = text(
    "insert into boogoo_corehr.hhr_org_emp_rpt_lvl(tenant_id, hhr_empid, hhr_emp_rcd, hhr_report_type, hhr_report_position, "
    "hhr_rpt_emp_id, hhr_rpt_emp_rcd, hhr_rpt_lvl) values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8) ")

# dept_lvl_field_map = {'L10': 'hhr_lvl_group', 'L20': 'hhr_lvl_plate', 'L30': 'hhr_lvl_company', 'L40': 'hhr_lvl_system',
#                       'L50': 'hhr_lvl_dept', 'L60': 'hhr_lvl_first_module', 'L61': 'hhr_lvl_second_module',
#                       'L70': 'hhr_lvl_team',
#                       '': ''}

dept_lvl_field_map = {'L10': 'hhr_lvl_group', 'L11': 'hhr_lvl_plate',
                      'L20': 'hhr_lvl_company', 'L21': 'hhr_lvl_system',
                      'L22': 'hhr_lvl_division',
                      'L30': 'hhr_lvl_dept', 'L40': 'hhr_lvl_first_module',
                      'L31': 'hhr_lvl_office', 'L32': 'hhr_lvl_room',
                      'L50': 'hhr_lvl_second_module', 'L60': 'hhr_lvl_team',
                      'L61': 'hhr_lvl_second_team', '': ''}


def get_cfg_sw_val(conn, tenant_id, sw_cd):
    cfg_sw_sql = text(
        "select hhr_cfg_sw_val from boogoo_foundation.hhr_switch_cfg where tenant_id = :b1 and hhr_cfg_sw_code = :b2 ")
    cfg_sw_row = conn.execute(cfg_sw_sql, b1=tenant_id, b2=sw_cd).fetchone()
    if cfg_sw_row is not None:
        sw_val = cfg_sw_row['hhr_cfg_sw_val']
    else:
        sw_val = 'N'
    return sw_val


def get_child_depts(tenant_id, effdt, dept_id, dept_future_sw, dba=None):
    result_lst = []
    if dba is None:
        dba = DataBaseAlchemy()

    result_dic = dict()
    result_dic['dept_id'] = dept_id
    result_dic['children'] = [dept_id]

    s1 = text(
        "select d.hhr_dept_schema_left, d.hhr_dept_schema_right, d.hhr_efft_date, d.hhr_root_dept_code "
        "from boogoo_corehr.hhr_org_dept_schema d where d.tenant_id = :b1 and d.hhr_dept_code = :b3 and d.hhr_tree_code = 'ORG-DEPT-TREE' "
        "and d.hhr_efft_date = (select max(d1.hhr_efft_date) from boogoo_corehr.hhr_org_tree d1 "
        "where d1.tenant_id = d.tenant_id and d1.hhr_tree_code = d.hhr_tree_code "
        "and d1.hhr_efft_date <= :b2) ")
    child_sql = text("select d.hhr_dept_code from boogoo_corehr.hhr_org_dept_schema d where d.tenant_id = :b1 "
                     "and d.hhr_tree_code = 'ORG-DEPT-TREE' and d.hhr_efft_date = :b2 "
                     "and d.hhr_dept_schema_left > :b3 and d.hhr_dept_schema_right < :b4 "
                     "and d.hhr_root_dept_code = :b5 order by 1")

    row1 = dba.conn.execute(s1, b1=tenant_id, b2=effdt, b3=dept_id).fetchone()
    if row1 is not None:
        left = row1['hhr_dept_schema_left']
        right = row1['hhr_dept_schema_right']
        tree_effdt = row1['hhr_efft_date']
        root_cd = row1['hhr_root_dept_code']

        rs = dba.conn.execute(child_sql, b1=tenant_id, b2=tree_effdt, b3=left, b4=right, b5=root_cd).fetchall()
        for row2 in rs:
            result_dic['children'].append(row2['hhr_dept_code'])
        result_dic['effdt'] = tree_effdt
    else:
        s1 = text("select d.hhr_efft_date from boogoo_corehr.hhr_org_dept_schema d where d.tenant_id = :b1 "
                  "and d.hhr_tree_code = 'ORG-DEPT-TREE' and d.hhr_dept_code = :b3 "
                  "and ( d.hhr_efft_date = (select max(d1.hhr_efft_date) from boogoo_corehr.hhr_org_tree d1 "
                  " where d1.tenant_id = d.tenant_id and d1.hhr_tree_code = d.hhr_tree_code and d1.hhr_efft_date <= :b2) "
                  "or d.hhr_efft_date > :b2 ) order by d.hhr_efft_date ")
        row1 = dba.conn.execute(s1, b1=tenant_id, b2=effdt, b3=dept_id).fetchone()
        if row1 is not None:
            dept_tree_effdt = row1['hhr_efft_date']
            if dept_future_sw != 'Y':
                if dept_tree_effdt > effdt:
                    dept_tree_effdt = effdt
        else:
            dept_tree_effdt = effdt
        result_dic['effdt'] = dept_tree_effdt

    result_dic['children'].sort()

    result_lst.append(result_dic)

    # 合并未来所有部门树
    if dept_future_sw == 'Y':
        s3 = text(
            "select d.hhr_dept_schema_left, d.hhr_dept_schema_right, d.hhr_efft_date, d.hhr_root_dept_code "
            "from boogoo_corehr.hhr_org_dept_schema d where d.tenant_id = :b1 and d.hhr_dept_code = :b3 and d.hhr_tree_code = 'ORG-DEPT-TREE' "
            "and d.hhr_efft_date > :b2 order by d.hhr_efft_date ")
        rs3 = dba.conn.execute(s3, b1=tenant_id, b2=effdt, b3=dept_id).fetchall()
        for row3 in rs3:
            fu_result_dic = dict()
            fu_result_dic['dept_id'] = dept_id
            fu_result_dic['children'] = []
            left = row3['hhr_dept_schema_left']
            right = row3['hhr_dept_schema_right']
            tree_effdt = row3['hhr_efft_date']
            root_cd = row3['hhr_root_dept_code']
            fu_result_dic['effdt'] = tree_effdt
            rs_child = dba.conn.execute(child_sql, b1=tenant_id, b2=tree_effdt, b3=left, b4=right,
                                        b5=root_cd).fetchall()
            for child in rs_child:
                child_dept_cd = child['hhr_dept_code']

                # 判断子部门是否已经存在，如果已存在，则不再添加该子部门
                exists = False
                for row_dic in result_lst:
                    if child_dept_cd in row_dic['children']:
                        exists = True
                        break
                if not exists:
                    fu_result_dic['children'].append(child_dept_cd)
            fu_result_dic['children'].sort()

            result_lst.append(fu_result_dic)

    return result_lst


def build_posn_rpt_data(conn, tenant_id, eff_dt, root_posn, rpt_to_type, rpt_to_posn, level, rpt_dic,
                        processed_pos_lst):
    if rpt_to_posn not in processed_pos_lst:
        processed_pos_lst.append(rpt_to_posn)
    else:
        raise Exception("职位" + root_posn + "的汇报线有误")

    stmt = text(
        "insert into boogoo_corehr.hhr_org_position_lvl(tenant_id, hhr_posn_code, hhr_report_position, hhr_report_type, hhr_rpt_lvl) "
        "values(:b1, :b2, :b3, :b4, :b5)")

    if not rpt_to_posn:
        level = 0
        rpt_to_posn = ''

    conn.execute(stmt, b1=tenant_id, b2=root_posn, b3=rpt_to_posn, b4=rpt_to_type, b5=level)

    if rpt_to_posn:
        if rpt_to_posn in rpt_dic:
            rpt_lst = rpt_dic[rpt_to_posn]
            if rpt_to_type == '10':
                rpt_posn = rpt_lst[0]
            elif rpt_to_type == '20':
                rpt_posn = rpt_lst[1]
            else:
                rpt_posn = rpt_lst[0]
            if rpt_posn:
                level = level + 1
                build_posn_rpt_data(conn, tenant_id, eff_dt, root_posn, rpt_to_type, rpt_posn, level, rpt_dic,
                                    processed_pos_lst)


def upd_posn_rpt_data(conn, tenant_id, eff_dt):
    rpt_dic = {}
    sel_stmt = text(
        "select a.hhr_posn_code, a.hhr_direct_report_posn, a.hhr_dashed_report_posn from boogoo_corehr.hhr_org_posn a where a.tenant_id = :b1 "
        "and a.hhr_efft_date = (select max(a1.hhr_efft_date) from boogoo_corehr.hhr_org_posn a1 where a1.tenant_id = a.tenant_id "
        "and a1.hhr_posn_code = a.hhr_posn_code and a1.hhr_efft_date <= :b2) and a.hhr_status = 'Y' order by a.hhr_posn_code ")

    rs = conn.execute(sel_stmt, b1=tenant_id, b2=eff_dt).fetchall()
    for row in rs:
        posn_cd = row['hhr_posn_code']
        dir_rpt_posn = row['hhr_direct_report_posn']
        dot_rpt_posn = row['hhr_dashed_report_posn']
        rpt_dic[posn_cd] = [dir_rpt_posn, dot_rpt_posn]

    trans = conn.begin()
    del_stmt = text("delete from boogoo_corehr.hhr_org_position_lvl where tenant_id = :b1 ")
    try:
        conn.execute(del_stmt, b1=tenant_id)
    except OperationalError:
        trans.rollback()
        raise Exception("不能同时运行多个职位和人员层级刷新进程")

    for posn_cd, rpt_lst in rpt_dic.items():
        dir_rpt_posn = rpt_lst[0]
        dot_rpt_posn = rpt_lst[1]
        try:
            # 构建直线和虚线汇报关系数据
            processed_pos_dir_lst = [posn_cd]
            processed_pos_dot_lst = [posn_cd]
            build_posn_rpt_data(conn, tenant_id, eff_dt, posn_cd, '10', dir_rpt_posn, 1, rpt_dic, processed_pos_dir_lst)
            build_posn_rpt_data(conn, tenant_id, eff_dt, posn_cd, '20', dot_rpt_posn, 1, rpt_dic, processed_pos_dot_lst)
        except Exception as e:
            logging.error(e)

    trans.commit()
    return rpt_dic


def build_emp_rpt_line(conn, emp, rpt_posn, rpt_emp_id, rpt_emp_obj, rpt_to_type, level, emp_dic, posn_emp_dic,
                       posn_rpt_dic):
    tenant_id = emp.tenant_id
    emp_id = emp.emp_id
    emp_rcd = emp.emp_rcd
    actual_rpt_emp_id = rpt_emp_obj.emp_id
    actual_rpt_emp_rcd = rpt_emp_obj.emp_rcd

    if emp_id == actual_rpt_emp_id and emp_rcd == actual_rpt_emp_rcd:
        raise Exception("员工" + emp_id + "在任职记录" + str(emp_rcd) + "上的汇报线有误")

    high_rpt_posn = ''
    high_rpt_emp_id = ''
    high_rpt_emp_rcd = 0
    if rpt_to_type == '10':
        high_rpt_posn = rpt_emp_obj.dir_rpt_posn
        high_rpt_emp_id = rpt_emp_obj.direct_empid
        high_rpt_emp_rcd = rpt_emp_obj.direct_emp_rcd
    elif rpt_to_type == '20':
        high_rpt_posn = rpt_emp_obj.dot_rpt_posn
        high_rpt_emp_id = rpt_emp_obj.indirect_empid
        high_rpt_emp_rcd = rpt_emp_obj.indirect_emp_rcd

    # 如果维护了汇报人，则忽略汇报职位
    if rpt_emp_id:
        rpt_posn = ''

    if rpt_posn is None:
        rpt_posn = ''
    if actual_rpt_emp_id is None:
        actual_rpt_emp_id = ''
    if actual_rpt_emp_rcd is None:
        actual_rpt_emp_rcd = 0

    if level == 1 and rpt_posn == '' and actual_rpt_emp_id == '':
        level = 0
    if level > 1 and rpt_posn == '' and actual_rpt_emp_id == '':
        pass
    else:
        sel_row = conn.execute(emp_rpt_chk_stmt, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=rpt_to_type, b5=rpt_posn,
                               b6=actual_rpt_emp_id, b7=actual_rpt_emp_rcd).fetchone()
        if sel_row is None:
            conn.execute(emp_rtp_ins_stmt, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=rpt_to_type, b5=rpt_posn,
                         b6=actual_rpt_emp_id, b7=actual_rpt_emp_rcd, b8=level)

        # 如果只维护了汇报职位，但是职位上没人，则继续往上查找
        if rpt_posn != '' and rpt_emp_obj.emp_id == '':
            if rpt_posn in posn_rpt_dic:
                rpt_list = posn_rpt_dic[rpt_posn]
                if rpt_to_type == '10':
                    high_rpt_posn = rpt_list[0]
                elif rpt_to_type == '20':
                    high_rpt_posn = rpt_list[1]

        if high_rpt_emp_id != '' or high_rpt_posn != '':
            high_rpt_emp_lst = []
            if high_rpt_emp_id:
                high_rpt_emp_obj = emp_dic.get((high_rpt_emp_id, high_rpt_emp_rcd), EmployeeRecord())
                high_rpt_emp_lst.append(high_rpt_emp_obj)
            else:
                if high_rpt_posn:
                    if high_rpt_posn in posn_emp_dic:
                        high_rpt_emp_lst = posn_emp_dic[high_rpt_posn]
                    else:
                        high_rpt_emp_lst = [EmployeeRecord()]
                else:
                    high_rpt_emp_lst = [EmployeeRecord()]

            level = level + 1
            if level >= 30:
                raise Exception("员工" + emp_id + "在任职记录" + str(emp_rcd) + "上的汇报线有误，请仔细核对其所有汇报层级数据。")

            for high_rpt_emp_obj in high_rpt_emp_lst:
                build_emp_rpt_line(conn, emp, high_rpt_posn, high_rpt_emp_id, high_rpt_emp_obj, rpt_to_type, level,
                                   emp_dic, posn_emp_dic, posn_rpt_dic)


def upd_emp_rpt_data(conn, tenant_id, eff_dt, posn_rpt_dic):
    """刷新员工汇报关系数据"""

    sel_stmt = text(
        "select a.hhr_empid, a.hhr_emp_rcd, a.hhr_posn_code, a.hhr_direct_rpt_posn, a.hhr_dotted_rpt_posn, a.hhr_status,  "
        "a.hhr_base_on_posn_sw, a.hhr_direct_rpt_empid, a.hhr_direct_rpt_emp_rcd, a.hhr_dotted_rpt_empid, a.hhr_dotted_rpt_emp_rcd "
        "from boogoo_corehr.hhr_org_per_jobdata a "
        "  where a.tenant_id = :b1 and a.hhr_efft_date <= :b2  "
        "order by a.hhr_empid, a.hhr_emp_rcd, a.hhr_efft_date desc, a.hhr_efft_seq desc ")
    rs = conn.execute(sel_stmt, b1=tenant_id, b2=eff_dt).fetchall()
    temp_tst = []
    for row in rs:
        try:
            emp_id = row['hhr_empid']
            emp_rcd = row['hhr_emp_rcd']
            if (emp_id, emp_rcd) not in temp_tst:
                temp_tst.append((emp_id, emp_rcd))
            else:
                continue
            hr_status = row['hhr_status']
            if hr_status != 'Y':
                continue
            posn_cd = row['hhr_posn_code']
            dir_rpt_posn = row['hhr_direct_rpt_posn']
            dot_rpt_posn = row['hhr_dotted_rpt_posn']

            base_on_posn_sw = row['hhr_base_on_posn_sw']
            direct_empid = row['hhr_direct_rpt_empid']
            direct_emp_rcd = row['hhr_direct_rpt_emp_rcd']
            indirect_empid = row['hhr_dotted_rpt_empid']
            indirect_emp_rcd = row['hhr_dotted_rpt_emp_rcd']

            if (posn_cd == '' or posn_cd is None) and base_on_posn_sw == 'Y':
                raise Exception("员工" + emp_id + "在任职记录号" + emp_rcd + "上是基于职位管理，但未维护职位！")

            v_dic = {'tenant_id': tenant_id, 'emp_id': emp_id, 'emp_rcd': emp_rcd, 'posn_cd': posn_cd,
                     'base_on_posn_sw': base_on_posn_sw,
                     'dir_rpt_posn': dir_rpt_posn,
                     'dot_rpt_posn': dot_rpt_posn, 'direct_empid': direct_empid, 'direct_emp_rcd': direct_emp_rcd,
                     'indirect_empid': indirect_empid, 'indirect_emp_rcd': indirect_emp_rcd}
            empRcdInfo = EmployeeRecord(**v_dic)

            # 一个职位可能对应多个任职记录
            if posn_cd:
                if posn_cd not in posn_emp_dic:
                    posn_emp_dic[posn_cd] = []
                posn_emp_dic[posn_cd].append(empRcdInfo)

                if posn_cd == dir_rpt_posn or posn_cd == dot_rpt_posn:
                    raise Exception("员工" + emp_id + "的职位" + posn_cd + "的汇报关系有误")
            emp_dic[(emp_id, emp_rcd)] = empRcdInfo
        except Exception as e1:
            logging.error(e1)

    # 开始处理每一个任职记录
    trans = conn.begin()
    del_stmt = text("delete from boogoo_corehr.hhr_org_emp_rpt_lvl where tenant_id = :b1 ")
    try:
        conn.execute(del_stmt, b1=tenant_id)
    except OperationalError:
        trans.rollback()
        raise Exception("不能同时运行多个职位和人员层级刷新进程")

    for emp in emp_dic.values():
        try:
            if emp.base_on_posn_sw == '' or emp.base_on_posn_sw is None:
                raise Exception("员工" + emp.emp_id + "的基于职位管理选项不能为空")

            for rpt_type in ['10', '20']:
                rpt_emp_lst = []
                if rpt_type == '10':
                    rpt_empid = emp.direct_empid
                    rpt_emp_rcd = emp.direct_emp_rcd
                    rpt_posn = emp.dir_rpt_posn
                else:
                    rpt_empid = emp.indirect_empid
                    rpt_emp_rcd = emp.indirect_emp_rcd
                    rpt_posn = emp.dot_rpt_posn

                # 如果维护了汇报人则忽略汇报职位，取汇报人
                if rpt_empid:
                    rpt_emp_obj = emp_dic.get((rpt_empid, rpt_emp_rcd), EmployeeRecord())
                    rpt_emp_lst.append(rpt_emp_obj)
                else:
                    # 如果维护了汇报职位
                    if rpt_posn:
                        if rpt_posn in posn_emp_dic:
                            rpt_emp_lst = posn_emp_dic[rpt_posn]
                        # 如果汇报职位上无人
                        else:
                            rpt_emp_lst = [EmployeeRecord()]
                    # 无上级
                    else:
                        rpt_emp_lst = [EmployeeRecord()]

                for rpt_emp_obj in rpt_emp_lst:
                    build_emp_rpt_line(conn, emp, rpt_posn, rpt_empid, rpt_emp_obj, rpt_type, 1, emp_dic, posn_emp_dic,
                                       posn_rpt_dic)
        except Exception as e2:
            logging.error(e2)

    trans.commit()


def get_dept_lang_desc_dic(conn, tenant_id, dept_cd, eff_dt):
    """获取每个部门的多语言描述"""
    dept_desc_dic = {}
    stmt = text(
        "select distinct d1.lang, d1.hhr_dept_name from boogoo_corehr.hhr_org_dept d left join boogoo_corehr.hhr_org_dept_tl d1 on d1.id = d.id "
        "where d.tenant_id = :b1 and d.hhr_dept_code = :b2 "
        "and d.hhr_efft_date = (select max(dm.hhr_efft_date) from boogoo_corehr.hhr_org_dept dm where dm.tenant_id = d.tenant_id"
        "   and dm.hhr_dept_code = d.hhr_dept_code and dm.hhr_efft_date <= :b3) ")
    rs = conn.execute(stmt, b1=tenant_id, b2=dept_cd, b3=eff_dt).fetchall()
    for row in rs:
        lang_cd = row['lang']
        dept_name = row['hhr_dept_name']
        dept_desc_dic[lang_cd] = dept_name
    return dept_desc_dic


def upd_dept_desc_result_dic(conn, tenant_id, dept_cd, eff_dt, dept_desc_result_dic):
    dept_lang_desc_dic = get_dept_lang_desc_dic(conn, tenant_id, dept_cd, eff_dt)
    for lang_cd, dept_name in dept_lang_desc_dic.items():
        if lang_cd in dept_desc_result_dic:
            dept_desc_result_dic[lang_cd].append(dept_name)


def build_dept_rpt_data(conn, tenant_id, eff_dt, root_dept, rpt_to_dept, level, rpt_dic, dept_desc_result_dic):
    if rpt_to_dept in rpt_dic:
        parent_dept_lvl = rpt_dic[rpt_to_dept][1]
        parent_cost_center = rpt_dic[rpt_to_dept][2]
        parent_inner_order = rpt_dic[rpt_to_dept][3]
        parent_supplier = rpt_dic[rpt_to_dept][4]
        parent_pay_group = rpt_dic[rpt_to_dept][5]
        parent_scope = rpt_dic[rpt_to_dept][6]
        parent_tax_area = rpt_dic[rpt_to_dept][7]
    else:
        parent_dept_lvl = ''
        parent_cost_center = ''
        parent_inner_order = ''
        parent_supplier = ''
        parent_pay_group = ''
        parent_scope = ''
        parent_tax_area = ''

    ins_lvl_stmt = text(
        "insert into boogoo_corehr.hhr_org_dept_lvl(tenant_id, hhr_dept_code, hhr_efft_date, hhr_parent_dept, hhr_rpt_lvl) "
        "values(:b1, :b2, :b3, :b4, :b5)")
    cost_center = rpt_dic[root_dept][2]
    inner_order = rpt_dic[root_dept][3]
    supplier = rpt_dic[root_dept][4]
    pay_group = rpt_dic[root_dept][5]
    scope = rpt_dic[root_dept][6]
    tax_area = rpt_dic[root_dept][7]
    # 插入部门节点本身
    if level == 1:
        conn.execute(ins_lvl_stmt, b1=tenant_id, b2=root_dept, b3=eff_dt, b4=root_dept, b5=0)

        cur_dept_lvl = rpt_dic[root_dept][1]
        if dept_lvl_field_map[cur_dept_lvl]:
            ins_lvl_info_s = "insert into boogoo_corehr.hhr_org_dept_lvl_info(tenant_id, hhr_dept_code, hhr_efft_date, hhr_dept_lvl, " + \
                             dept_lvl_field_map[
                                 cur_dept_lvl] + ", hhr_cost_center_code, " \
                                                 "hhr_org_dept_attr09, hhr_org_dept_attr10, hhr_org_dept_attr11, hhr_org_dept_attr12, hhr_org_dept_attr13) " \
                                                 "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10, :b11) "
            conn.execute(text(ins_lvl_info_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=cur_dept_lvl, b5=root_dept,
                         b6=cost_center, b7=inner_order, b8=supplier, b9=pay_group, b10=scope, b11=tax_area)
        else:
            ins_lvl_info_s = "insert into boogoo_corehr.hhr_org_dept_lvl_info(tenant_id, hhr_dept_code, hhr_efft_date, hhr_dept_lvl, hhr_cost_center_code, " \
                             "hhr_org_dept_attr09, hhr_org_dept_attr10, hhr_org_dept_attr11, hhr_org_dept_attr12, hhr_org_dept_attr13) " \
                             "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10) "
            conn.execute(text(ins_lvl_info_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=cur_dept_lvl, b5=cost_center,
                         b6=inner_order, b7=supplier, b8=pay_group, b9=scope, b10=tax_area)

    # 如果成本中心为空，则继续往上查找，直到找到成本中心
    if cost_center == '' and parent_cost_center != '':
        upd_cst_center_s = "update boogoo_corehr.hhr_org_dept_lvl_info set hhr_cost_center_code = :b4 " \
                           "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_cst_center_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_cost_center)
        rpt_dic[root_dept][2] = parent_cost_center

    if inner_order == '' and parent_inner_order != '':
        upd_s = "update boogoo_corehr.hhr_org_dept_lvl_info set hhr_org_dept_attr09 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_inner_order)
        rpt_dic[root_dept][3] = parent_inner_order

    if supplier == '' and parent_supplier != '':
        upd_s = "update boogoo_corehr.hhr_org_dept_lvl_info set hhr_org_dept_attr10 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_supplier)
        rpt_dic[root_dept][4] = parent_supplier

    if pay_group == '' and parent_pay_group != '':
        upd_s = "update boogoo_corehr.hhr_org_dept_lvl_info set hhr_org_dept_attr11 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_pay_group)
        rpt_dic[root_dept][5] = parent_pay_group

    if scope == '' and parent_scope != '':
        upd_s = "update boogoo_corehr.hhr_org_dept_lvl_info set hhr_org_dept_attr12 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_scope)
        rpt_dic[root_dept][6] = parent_scope

    if tax_area == '' and parent_tax_area != '':
        upd_s = "update boogoo_corehr.hhr_org_dept_lvl_info set hhr_org_dept_attr13 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_tax_area)
        rpt_dic[root_dept][7] = parent_tax_area

    if dept_lvl_field_map[parent_dept_lvl]:
        upd_lvl_info_s = "update boogoo_corehr.hhr_org_dept_lvl_info set " + dept_lvl_field_map[
            parent_dept_lvl] + " = :b4 where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_lvl_info_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=rpt_to_dept)

    if rpt_to_dept and root_dept != rpt_to_dept:
        conn.execute(ins_lvl_stmt, b1=tenant_id, b2=root_dept, b3=eff_dt, b4=rpt_to_dept, b5=level)
        upd_dept_desc_result_dic(conn, tenant_id, rpt_to_dept, eff_dt, dept_desc_result_dic)
        if rpt_to_dept in rpt_dic:
            high_rpt_to_dept = rpt_dic[rpt_to_dept][0]
            if high_rpt_to_dept:
                level = level + 1
                build_dept_rpt_data(conn, tenant_id, eff_dt, root_dept, high_rpt_to_dept, level, rpt_dic,
                                    dept_desc_result_dic)


def build_cur_dept_rpt_data(conn, tenant_id, eff_dt, root_dept, rpt_to_dept, level, rpt_dic):
    if rpt_to_dept in rpt_dic:
        parent_dept_lvl = rpt_dic[rpt_to_dept][1]
        parent_cost_center = rpt_dic[rpt_to_dept][2]
        parent_inner_order = rpt_dic[rpt_to_dept][3]
        parent_supplier = rpt_dic[rpt_to_dept][4]
        parent_pay_group = rpt_dic[rpt_to_dept][5]
        parent_scope = rpt_dic[rpt_to_dept][6]
        parent_tax_area = rpt_dic[rpt_to_dept][7]
    else:
        parent_dept_lvl = ''
        parent_cost_center = ''
        parent_inner_order = ''
        parent_supplier = ''
        parent_pay_group = ''
        parent_scope = ''
        parent_tax_area = ''

    ins_lvl_stmt = text(
        "insert into boogoo_corehr.hhr_org_cur_dept_lvl(tenant_id, hhr_dept_code, hhr_efft_date, hhr_parent_dept, hhr_rpt_lvl) "
        "values(:b1, :b2, :b3, :b4, :b5)")

    cost_center = rpt_dic[root_dept][2]
    inner_order = rpt_dic[root_dept][3]
    supplier = rpt_dic[root_dept][4]
    pay_group = rpt_dic[root_dept][5]
    scope = rpt_dic[root_dept][6]
    tax_area = rpt_dic[root_dept][7]
    # 插入部门节点本身
    if level == 1:
        conn.execute(ins_lvl_stmt, b1=tenant_id, b2=root_dept, b3=eff_dt, b4=root_dept, b5=0)

        cur_dept_lvl = rpt_dic[root_dept][1]
        if cur_dept_lvl in dept_lvl_field_map:
            if dept_lvl_field_map[cur_dept_lvl]:
                ins_lvl_info_s = "insert into boogoo_corehr.hhr_org_cur_dept_lvl_info(tenant_id, hhr_dept_code, hhr_efft_date, hhr_dept_lvl, " + \
                                 dept_lvl_field_map[
                                     cur_dept_lvl] + ", hhr_cost_center_code, " \
                                                     "hhr_org_dept_attr09, hhr_org_dept_attr10, hhr_org_dept_attr11, hhr_org_dept_attr12, hhr_org_dept_attr13) " \
                                                     "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10, :b11) "
                conn.execute(text(ins_lvl_info_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=cur_dept_lvl, b5=root_dept,
                             b6=cost_center, b7=inner_order, b8=supplier, b9=pay_group, b10=scope, b11=tax_area)
            else:
                ins_lvl_info_s = "insert into boogoo_corehr.hhr_org_cur_dept_lvl_info(tenant_id, hhr_dept_code, hhr_efft_date, hhr_dept_lvl, hhr_cost_center_code, " \
                                 "hhr_org_dept_attr09, hhr_org_dept_attr10, hhr_org_dept_attr11, hhr_org_dept_attr12, hhr_org_dept_attr13) " \
                                 "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10) "
                conn.execute(text(ins_lvl_info_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=cur_dept_lvl,
                             b5=cost_center, b6=inner_order, b7=supplier, b8=pay_group, b9=scope, b10=tax_area)

    # 如果成本中心为空，则继续往上查找，直到找到成本中心
    if cost_center == '' and parent_cost_center != '':
        upd_cst_center_s = "update boogoo_corehr.hhr_org_cur_dept_lvl_info set hhr_cost_center_code = :b4 " \
                           "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_cst_center_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_cost_center)
        rpt_dic[root_dept][2] = parent_cost_center

    if inner_order == '' and parent_inner_order != '':
        upd_s = "update boogoo_corehr.hhr_org_cur_dept_lvl_info set hhr_org_dept_attr09 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_inner_order)
        rpt_dic[root_dept][3] = parent_inner_order

    if supplier == '' and parent_supplier != '':
        upd_s = "update boogoo_corehr.hhr_org_cur_dept_lvl_info set hhr_org_dept_attr10 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_supplier)
        rpt_dic[root_dept][4] = parent_supplier

    if pay_group == '' and parent_pay_group != '':
        upd_s = "update boogoo_corehr.hhr_org_cur_dept_lvl_info set hhr_org_dept_attr11 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_pay_group)
        rpt_dic[root_dept][5] = parent_pay_group

    if scope == '' and parent_scope != '':
        upd_s = "update boogoo_corehr.hhr_org_cur_dept_lvl_info set hhr_org_dept_attr12 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_scope)
        rpt_dic[root_dept][6] = parent_scope

    if tax_area == '' and parent_tax_area != '':
        upd_s = "update boogoo_corehr.hhr_org_cur_dept_lvl_info set hhr_org_dept_attr13 = :b4 " \
                "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=parent_tax_area)
        rpt_dic[root_dept][7] = parent_tax_area

    if dept_lvl_field_map[parent_dept_lvl]:
        upd_lvl_info_s = "update boogoo_corehr.hhr_org_cur_dept_lvl_info set " + dept_lvl_field_map[parent_dept_lvl] + \
                         " = :b4 where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 "
        conn.execute(text(upd_lvl_info_s), b1=tenant_id, b2=root_dept, b3=eff_dt, b4=rpt_to_dept)

    if rpt_to_dept and root_dept != rpt_to_dept:
        conn.execute(ins_lvl_stmt, b1=tenant_id, b2=root_dept, b3=eff_dt, b4=rpt_to_dept, b5=level)
        if rpt_to_dept in rpt_dic:
            high_rpt_to_dept = rpt_dic[rpt_to_dept][0]
            if high_rpt_to_dept:
                level = level + 1
                build_cur_dept_rpt_data(conn, tenant_id, eff_dt, root_dept, high_rpt_to_dept, level, rpt_dic)


def build_dept_desc_data(conn, tenant_id, eff_dt, dept_cd, dept_desc_result_dic, sep_value):
    ins_stmt = text(
        "insert into boogoo_corehr.hhr_org_dept_full_desc(tenant_id, hhr_dept_code, hhr_efft_date, lang, hhr_dept_name, hhr_dept_detail_desc) "
        "values(:b1, :b2, :b3, :b4, :b5, :b6) ")
    for lang_cd, desc_lst in dept_desc_result_dic.items():
        dept_name = desc_lst[0]
        desc_lst.reverse()
        dept_full_desc = sep_value.join(desc_lst)
        conn.execute(ins_stmt, b1=tenant_id, b2=dept_cd, b3=eff_dt, b4=lang_cd, b5=dept_name, b6=dept_full_desc)


def get_dept_desc_separator(conn, tenant_id):
    sep_value = "|"
    sep_stmt = text(
        "SELECT b.`value` As seq_value FROM hzero_platform.hpfm_profile a JOIN hzero_platform.hpfm_profile_value b ON b.profile_id = a.profile_id and b.level_code = 'GLOBAL' "
        "and b.level_value = 'GLOBAL' WHERE a.tenant_id = :b1 AND a.profile_name = 'HHR.COREHR.DEPT.FULL.DESC.SEP'")
    result = conn.execute(sep_stmt, b1=tenant_id).fetchone()
    if result is not None:
        sep_value = result['seq_value']
    return sep_value


def get_dept_lvl_upd_date_limit(conn, tenant_id):
    limit_value = 5
    stmt = text(
        "SELECT b.`value` As limit_value FROM hzero_platform.hpfm_profile a JOIN hzero_platform.hpfm_profile_value b ON b.profile_id = a.profile_id and b.level_code = 'GLOBAL' "
        "and b.level_value = 'GLOBAL' WHERE a.tenant_id = :b1 AND a.profile_name = 'HHR.COREHR.DEPT_LVL_UPD_LIMIT'")
    result = conn.execute(stmt, b1=tenant_id).fetchone()
    if result is not None:
        limit_value = result['limit_value']
    return limit_value


def get_dept_lvl_upd_date_str(conn, tenant_id):
    date_list_str = ''
    stmt = text(
        "SELECT b.`value` As date_list_str FROM hzero_platform.hpfm_profile a JOIN hzero_platform.hpfm_profile_value b ON b.profile_id = a.profile_id and b.level_code = 'GLOBAL' "
        "and b.level_value = 'GLOBAL' WHERE a.tenant_id = :b1 AND a.profile_name = 'HHR.COREHR.DEPT_LVL_UPD_LIST'")
    result = conn.execute(stmt, b1=tenant_id).fetchone()
    if result is not None:
        date_list_str = result['date_list_str']
    return date_list_str


def upd_dept_rpt_data(conn, tenant_id, eff_dt, lang_template_dic, isCur=False):
    """刷新部门层级关系数据"""
    sep_value = get_dept_desc_separator(conn, tenant_id)
    rpt_dic = {}
    # sel_stmt = text(
    #     "select a.hhr_dept_code, a.hhr_parent_dept, a.hhr_dept_level from boogoo_corehr.hhr_org_dept a where a.tenant_id = :b1 "
    #     "and :b2 between a.hhr_efft_date and a.hhr_efft_end_date and a.hhr_status = 'Y' order by a.hhr_dept_code ")
    sel_stmt = text(
        "select a.hhr_dept_code, a.hhr_parent_dept, a.hhr_dept_level, a.hhr_cost_center_code, "
        "hhr_org_dept_attr09, hhr_org_dept_attr10, hhr_org_dept_attr11, hhr_org_dept_attr12, hhr_org_dept_attr13 "
        "from boogoo_corehr.hhr_org_dept a where a.tenant_id = :b1 "
        "and :b2 between a.hhr_efft_date and a.hhr_efft_end_date order by a.hhr_dept_code ")
    rs = conn.execute(sel_stmt, b1=tenant_id, b2=eff_dt).fetchall()
    for row in rs:
        dept_cd = row['hhr_dept_code']
        rpt_to_dept = row['hhr_parent_dept']
        dept_level = row['hhr_dept_level']
        cost_center = row['hhr_cost_center_code']
        # 内部订单
        inner_order = row['hhr_org_dept_attr09']
        # 供应商
        supplier = row['hhr_org_dept_attr10']
        # 薪资组
        pay_group = row['hhr_org_dept_attr11']
        # 适用范围
        scope = row['hhr_org_dept_attr12']
        # 纳税地
        tax_area = row['hhr_org_dept_attr13']
        if rpt_to_dept is None:
            rpt_to_dept = ''
        if dept_level is None:
            dept_level = ''
        if cost_center is None:
            cost_center = ''
        if inner_order is None:
            inner_order = ''
        if supplier is None:
            supplier = ''
        if pay_group is None:
            pay_group = ''
        if scope is None:
            scope = ''
        if tax_area is None:
            tax_area = ''
        # 顶级部门无上级部门
        if dept_cd == rpt_to_dept:
            rpt_to_dept = ''
        rpt_dic[dept_cd] = [rpt_to_dept, dept_level, cost_center, inner_order, supplier, pay_group, scope, tax_area]

    trans = conn.begin()
    try:
        if isCur:
            del_cur_stmt = text("delete from boogoo_corehr.hhr_org_cur_dept_lvl where tenant_id = :b1 ")
            del_cur_lvl_info_stmt = text("delete from boogoo_corehr.hhr_org_cur_dept_lvl_info where tenant_id = :b1 ")
            conn.execute(del_cur_stmt, b1=tenant_id)
            conn.execute(del_cur_lvl_info_stmt, b1=tenant_id)
            for dept_cd, dept_inf_lst in rpt_dic.items():
                build_cur_dept_rpt_data(conn, tenant_id, eff_dt, dept_cd, dept_inf_lst[0], 1, rpt_dic)
        else:
            del_lvl_stmt = text("delete from boogoo_corehr.hhr_org_dept_lvl where tenant_id = :b1 and hhr_efft_date = :b2 ")
            del_desc_stmt = text(
                "delete from boogoo_corehr.hhr_org_dept_full_desc where tenant_id = :b1 and hhr_efft_date = :b2 ")
            del_lvl_info_stmt = text(
                "delete from boogoo_corehr.hhr_org_dept_lvl_info where tenant_id = :b1 and hhr_efft_date = :b2 ")
            conn.execute(del_lvl_stmt, b1=tenant_id, b2=eff_dt)
            conn.execute(del_desc_stmt, b1=tenant_id, b2=eff_dt)
            conn.execute(del_lvl_info_stmt, b1=tenant_id, b2=eff_dt)
            for dept_cd, dept_inf_lst in rpt_dic.items():
                dept_desc_result_dic = deepcopy(lang_template_dic)
                upd_dept_desc_result_dic(conn, tenant_id, dept_cd, eff_dt, dept_desc_result_dic)
                build_dept_rpt_data(conn, tenant_id, eff_dt, dept_cd, dept_inf_lst[0], 1, rpt_dic, dept_desc_result_dic)
                build_dept_desc_data(conn, tenant_id, eff_dt, dept_cd, dept_desc_result_dic, sep_value)

            insert_org_tree(conn, tenant_id, eff_dt)
        trans.commit()
    except Exception:
        trans.rollback()
        raise


def insert_org_tree(conn, tenant_id, eff_dt):
    s = text(
        "select 'Y' from boogoo_corehr.hhr_org_tree where tenant_id = :b1 and hhr_tree_code = 'ORG-DEPT-TREE' and hhr_efft_date = :b2 ")
    row = conn.execute(s, b1=tenant_id, b2=eff_dt).fetchone()
    if row is None:
        ins = text("INSERT INTO boogoo_corehr.hhr_org_tree (tenant_id, hhr_tree_code, hhr_efft_date, hhr_tree_type, "
                   "creation_date, created_by, last_updated_by, last_update_date) "
                   "VALUES (:b1, 'ORG-DEPT-TREE', :b2, 'ODT', :b3, :b4, :b5, :b6) ")
        conn.execute(ins, b1=tenant_id, b2=eff_dt, b3=get_current_dttm(), b4=2, b5=2, b6=get_current_dttm())


def refresh_rpt_data(conn, tenant_id, eff_dt):
    """刷新职位、人员层级关系"""
    global posn_emp_dic
    global emp_dic
    posn_emp_dic = {}
    emp_dic = {}
    posn_rpt_dic = upd_posn_rpt_data(conn, tenant_id, eff_dt)
    upd_emp_rpt_data(conn, tenant_id, eff_dt, posn_rpt_dic)


def get_def_role_id(conn):
    """获取默认角色对应的唯一ID"""

    def_role = 'HHR_SS_DEFAULT'
    role_sel = text("select id from hzero_platform.iam_role where code = :b1")
    def_role_row = conn.execute(role_sel, b1=def_role).fetchone()
    if def_role_row is not None:
        def_role_id = def_role_row['id']
    else:
        raise Exception("不存在默认角色HHR_SS_DEFAULT")
    return def_role_id


def create_role_for_user(conn, tenant_id, new_emp_id):
    """为指定用户创建角色"""

    def_role_id = get_def_role_id(conn)
    member_id_sel = text("select id from hzero_platform.iam_user where login_name = :b1 and organization_id = :b2")
    user_role_ins = text(
        "insert into hzero_platform.iam_member_role(role_id, member_id, member_type, source_id, source_type, "
        "h_assign_level, h_assign_level_value) values(:b1, :b2, :b3, :b4, :b5, :b6, :b7) ")

    member_id_row = conn.execute(member_id_sel, b1=new_emp_id, b2=tenant_id).fetchone()
    if member_id_row is not None:
        member_id = member_id_row['id']
        conn.execute(user_role_ins, b1=def_role_id, b2=member_id, b3='user', b4=tenant_id, b5='organization',
                     b6='organization', b7=tenant_id)


def create_new_user_acc(conn, tenant_id, new_emp_id, cur_date):
    """创建新用户账号"""

    init_pwd = '$2a$10$85L4lPocvM2/E2ru74bsbu.ZO2VBuKIbG5ke2G6FFLLGNzV6Juwc.'
    emp_name_sel = text(
        "select hhr_emp_name from boogoo_corehr.hhr_org_per_biog where tenant_id = :b1 and hhr_empid = :b2")

    user_ins = text(
        "insert into hzero_platform.iam_user(login_name, organization_id, hash_password, real_name, language, "
        "time_zone, last_password_updated_at, is_enabled, is_locked, user_type) "
        "values(:b1, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b9, :b10) ")

    emp_name_row = conn.execute(emp_name_sel, b1=tenant_id, b2=new_emp_id).fetchone()
    if emp_name_row is not None:
        hhr_emp_name = emp_name_row['hhr_emp_name']
    else:
        hhr_emp_name = new_emp_id

    conn.execute(user_ins, b1=new_emp_id, b2=tenant_id, b3=init_pwd, b4=hhr_emp_name,
                 b5='zh_CN', b6='GMT+8', b7=cur_date, b8=1, b9=0, b10='P')

    # 分配角色
    create_role_for_user(conn, tenant_id, new_emp_id)


def close_user_acc(conn, tenant_id, emp_id):
    """关闭用户账号"""

    user_sel = text("select 'Y' from hzero_platform.iam_user where login_name = :b1 and organization_id = :b2")
    row = conn.execute(user_sel, b1=emp_id, b2=tenant_id).fetchone()
    if row is not None:
        user_upd = text("update hzero_platform.iam_user set is_locked = 0, is_enabled = 0 where login_name = :b1 "
                        "and organization_id = :b2")
        conn.execute(user_upd, b1=emp_id, b2=tenant_id)


def upd_emp_account(conn, tenant_id, emp_id, eff_dt):
    """创建/注销员工账号"""

    cur_date = get_current_date()

    # 获取 1：新入职还没有创建账号的员工 并且 该员工的ID未与已有账号名冲突 或者 2：已离职的员工
    emp_sql_str = "select j.hhr_empid, j.hhr_status from boogoo_corehr.hhr_org_per_jobdata j where j.tenant_id = :b1 and j.hhr_job_indicator = 'P' " \
                  "and j.hhr_efft_date = (select max(j1.hhr_efft_date) from boogoo_corehr.hhr_org_per_jobdata j1 where j1.tenant_id = j.tenant_id " \
                  " and j1.hhr_empid = j.hhr_empid and j1.hhr_emp_rcd = j.hhr_emp_rcd and j1.hhr_efft_date <= :b2) " \
                  "and j.hhr_efft_seq = (select max(j2.hhr_efft_seq) from boogoo_corehr.hhr_org_per_jobdata j2 where j2.tenant_id = j.tenant_id " \
                  " and j2.hhr_empid = j.hhr_empid and j2.hhr_emp_rcd = j.hhr_emp_rcd and j2.hhr_efft_date = j.hhr_efft_date) " \
                  "and ( not exists(select 'Y' from hzero_platform.iam_user u where u.login_name = j.hhr_empid and u.organization_id = j.tenant_id) " \
                  " or j.hhr_status = 'N' ) "

    trans = conn.begin()
    try:
        if emp_id:
            emp_sql_str = emp_sql_str + " AND j.hhr_empid = :b3"
        rs = conn.execute(text(emp_sql_str), b1=tenant_id, b2=eff_dt, b3=emp_id).fetchall()
        for row in rs:
            each_emp_id = row['hhr_empid']
            hr_status = row['hhr_status']
            # 如果为新入职员工，则为其创建账号
            if hr_status == 'Y':
                # 新建用户并为其添加默认角色
                create_new_user_acc(conn, tenant_id, each_emp_id, cur_date)

            # 如果为离职员工，则注销其关联的用户账号
            else:
                close_user_acc(conn, tenant_id, each_emp_id)
        trans.commit()
    except Exception:
        trans.rollback()
        raise
