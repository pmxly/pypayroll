# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
import datetime


class PyFunction(FunctionObject):
    """
    Desc: 获取组织成本中心函数
    Author: David
    Date: 2021/03/16
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_ORG_COST_CENTER'
        self.country = 'CHN'
        self.desc = '获取组织成本中心函数'
        self.descENG = '获取组织成本中心函数'
        self.func_type = 'A'
        self.instructions = "获取组织成本中心。输入参数：组织编码、日期（未指定时默认历经期结束日期）; " \
                            "输出参数：成本中心、内部订单、供应商"
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
                'PA': ['dept_cd', 'to_date']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, dept_cd, to_date=None):
        if dept_cd is None or dept_cd == '':
            raise Exception("函数FC_GET_ORG_COST_CENTER的参数错误")
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
                    raise Exception("函数FC_GET_ORG_COST_CENTER的参数错误")
        elif not isinstance(to_date, datetime.date):
            raise Exception("函数FC_GET_ORG_COST_CENTER的参数错误")

        tree_efft_dic = gv.get_run_var_value('TREE_EFFT_DIC')

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id

        # 先从tree表中取到指定日期最新的生效日期，再从表hhr_org_dept_lvl_info中取值
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

        t = text(
            "select hhr_cost_center_code, hhr_org_dept_attr09, hhr_org_dept_attr10 from boogoo_corehr.hhr_org_dept_lvl_info "
            "where tenant_id = :b1 and hhr_dept_code = :b2 and hhr_efft_date = :b3 ")
        row = db.conn.execute(t, b1=tenant_id, b2=dept_cd, b3=tree_efft_dt).fetchone()
        if row is not None:
            return row['hhr_cost_center_code'], row['hhr_org_dept_attr09'], row['hhr_org_dept_attr10']
        else:
            return '', '', ''
