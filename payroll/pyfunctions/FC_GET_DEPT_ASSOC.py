# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
import datetime
from ..pyexecute.pycalculate.table.dept_assoc import DeptAssoc


class PyFunction(FunctionObject):
    """
    Desc: 获取组织关联信息函数
    Author: David
    Date: 2020/12/11
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_DEPT_ASSOC'
        self.country = 'CHN'
        self.desc = '获取组织关联信息函数'
        self.descENG = '获取组织关联信息函数'
        self.func_type = 'A'
        self.instructions = "获取组织关联信息函数。输入参数：组织、日期; 输出参数：组织关联信息对象（含所有字段）"
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
            raise Exception("函数FC_GET_DEPT_ASSOC的参数错误")

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
            raise Exception("函数FC_GET_DEPT_ASSOC的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id
        t = text("select * from boogoo_payroll.hhr_py_org_asso_info where tenant_id = :b1 and hhr_dept_code = :b2 and :b3 "
                 "between hhr_efft_date and hhr_efft_end_date")
        r = db.conn.execute(t, b1=tenant_id, b2=dept_cd, b3=to_date).fetchone()
        if r is not None:
            return DeptAssoc(**r)
        else:
            return DeptAssoc(**{})
