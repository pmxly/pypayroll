# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
import datetime
from ..pyexecute.pycalculate.table.emp_cst_cntr import EmpCostCenter


class PyFunction(FunctionObject):
    """
    Desc: 获取人员成本中心函数
    Author: David
    Date: 2021/03/16
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_EMP_COST_CENTER'
        self.country = 'CHN'
        self.desc = '获取人员成本中心函数'
        self.descENG = '获取人员成本中心函数'
        self.func_type = 'A'
        self.instructions = "获取人员成本中心。输入参数：日期（未指定时默认历经期结束日期）；输出参数：对象（包含字段：成本中心、内部订单、供应商、比例）列表"
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
    def func_exec(self, to_date=None):
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

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id

        # 根据日期从人员成本中心表中取值。
        # 先从表hhr_py_emp_cost_center中找到此员工对应日期的生效日期，再根据员工号、生效日期从表hhr_py_emp_cost_center_child中取记录。
        # 第一张表有记录，第二张表中可能也会没有记录。
        t = text("SELECT b.hhr_efft_date, b.hhr_cost_center_cd, b.hhr_ratio, b.hhr_inner_order, b.hhr_supp_desc FROM "
                 "boogoo_payroll.hhr_py_emp_cost_center a JOIN boogoo_payroll.hhr_py_emp_cost_center_child b ON a.tenant_id = b.tenant_id "
                 "AND a.hhr_empid = b.hhr_empid AND a.hhr_emp_rcd = b.hhr_emp_rcd AND a.hhr_efft_date = b.hhr_efft_date "
                 "WHERE a.tenant_id = :b1 AND a.hhr_empid = :b2 AND a.hhr_emp_rcd = :b3 "
                 "AND :b4 BETWEEN a.hhr_efft_date AND a.hhr_efft_end_date")
        rs = db.conn.execute(t, b1=tenant_id, b2=catalog.emp_id, b3=catalog.emp_rcd, b4=to_date).fetchall()
        if rs is not None:
            ecc_lst = []
            for r in rs:
                ecc = EmpCostCenter(**r)
                ecc_lst.append(ecc)
            return ecc_lst
        else:
            return [EmpCostCenter(**{})]
