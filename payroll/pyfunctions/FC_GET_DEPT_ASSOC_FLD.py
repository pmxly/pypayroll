# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
import datetime
from ..pyexecute.pycalculate.table.dept_assoc import DeptAssoc


class PyFunction(FunctionObject):
    """
    Desc: 获取字段在组织关联信息表中的值
    Author: David
    Date: 2020/12/11
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_DEPT_ASSOC_FLD'
        self.country = 'CHN'
        self.desc = '获取字段在组织关联信息表中的值'
        self.descENG = '获取字段在组织关联信息表中的值'
        self.func_type = 'A'
        self.instructions = "获取字段在组织关联信息表中的值。输入参数：组织、字段名、日期; 输出参数：字段的值"
        self.instructionsENG = self.instructions

        self.log_flag = gv.get_run_var_value('LOG_FLAG')
        if self.log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': [],
                'VR': [],
                'PA': ['dept_cd', 'field_name', 'to_date']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, dept_cd, field_name, to_date=None):
        if dept_cd is None or dept_cd == '':
            raise Exception("函数FC_GET_DEPT_ASSOC_FLD的参数错误")

        if field_name is None or field_name == '':
            raise Exception("函数FC_GET_DEPT_ASSOC_FLD的参数错误")

        if to_date is None:
            prd_end_dt = gv.get_var_value('VR_F_PERIOD_END')
            to_date = prd_end_dt

        if isinstance(to_date, str):
            try:
                to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")
            except (TypeError, ValueError):
                try:
                    to_date = datetime.datetime.strptime(to_date, "%Y/%m/%d")
                except (TypeError, ValueError):
                    raise Exception("函数FC_GET_DEPT_ASSOC_FLD的参数错误")

        elif not isinstance(to_date, datetime.date):
            raise Exception("函数FC_GET_DEPT_ASSOC_FLD的参数错误")

        tree_efft_dic = gv.get_run_var_value('TREE_EFFT_DIC')

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id

        # 获取树生效日期
        tree_key = str(tenant_id) + '_' + str(to_date)
        tree_efft_dt = None
        if tree_key in tree_efft_dic:
            tree_efft_dt = tree_efft_dic[tree_key]
        else:
            tree_efft_t = text(
                "select t.hhr_efft_date from boogoo_corehr.hhr_org_tree t where t.tenant_id = :b1 and t.hhr_tree_code = 'ORG-DEPT-TREE' AND t.hhr_efft_date = "
                "(SELECT max(t1.hhr_efft_date) from boogoo_corehr.hhr_org_tree t1 where t1.tenant_id = t.tenant_id AND t1.hhr_tree_code = t.hhr_tree_code "
                "and t1.hhr_efft_date <= :b2) ")
            tree_row = db.conn.execute(tree_efft_t, b1=tenant_id, b2=to_date).fetchone()
            if tree_row is not None:
                tree_efft_dt = tree_row['hhr_efft_date']
                tree_efft_dic[tree_key] = tree_efft_dt

        if tree_efft_dt is not None:
            check_t = text("select 'Y' from boogoo_corehr.hhr_org_dept_lvl d where d.tenant_id = :b1 and d.hhr_dept_code = :b2 AND d.hhr_efft_date = :b3")
            row = db.conn.execute(check_t, b1=tenant_id, b2=dept_cd, b3=tree_efft_dt).fetchone()
            if row is None:
                raise Exception("未获取到组织({})的记录，请等待组织模块完成树刷新。".format(dept_cd))
            asso_key = str(tenant_id) + '_' + dept_cd + '_' + str(tree_efft_dt) + '_' + field_name
            fld_asso_info_dic = gv.get_run_var_value('FLD_ASSO_INFO_DIC')
            if asso_key in fld_asso_info_dic:
                return fld_asso_info_dic[asso_key]
            else:
                # s = "select a." + field_name + " from boogoo_corehr.hhr_org_dept_lvl d JOIN boogoo_payroll.hhr_py_org_asso_info a ON a.tenant_id = d.tenant_id " \
                #                                "AND a.hhr_dept_code = d.hhr_parent_dept AND d.hhr_efft_date BETWEEN a.hhr_efft_date AND a.hhr_efft_end_date " \
                #                                "AND a." + field_name + " is not null AND a." + field_name + " != '' AND a." + field_name + " != '0' " \
                #                                "where d.tenant_id = :b1 AND d.hhr_dept_code = :b2 AND d.hhr_efft_date = :b3 order by d.hhr_rpt_lvl "
                s = "select a." + field_name + " from boogoo_corehr.hhr_org_dept_lvl d JOIN boogoo_payroll.hhr_py_org_asso_info a ON a.tenant_id = d.tenant_id " \
                                               "AND a.hhr_dept_code = d.hhr_parent_dept AND :b4 BETWEEN a.hhr_efft_date AND a.hhr_efft_end_date " \
                                               "AND a." + field_name + " is not null AND a." + field_name + " != '' AND a." + field_name + " != '0' " \
                                               "where d.tenant_id = :b1 AND d.hhr_dept_code = :b2 AND d.hhr_efft_date = :b3 order by d.hhr_rpt_lvl "
                t = text(s)
                r = db.conn.execute(t, b1=tenant_id, b2=dept_cd, b3=tree_efft_dt, b4=to_date).fetchone()
                if r is not None:
                    fld_asso_info_dic[asso_key] = r[field_name]
                    return r[field_name]
        return None
