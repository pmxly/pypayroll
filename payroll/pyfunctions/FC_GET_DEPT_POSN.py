# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
import datetime


class PyFunction(FunctionObject):
    """
    Desc: 获取组织、岗位函数
    Author: David
    Date: 2020/12/11
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_DEPT_POSN'
        self.country = 'CHN'
        self.desc = '获取组织、岗位函数'
        self.descENG = '获取组织、岗位函数'
        self.func_type = 'A'
        self.instructions = "获取组织、岗位函数。输入参数：日期; 输出参数：当前任职在对应日期的组织、岗位"
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
                'PA': ['to_date']
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
                to_date = datetime.datetime.strptime(to_date, "%Y/%m/%d")
        elif not isinstance(to_date, datetime.date):
            raise Exception("函数FC_GET_DEPT_POSN的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        t = text("SELECT j.hhr_dept_code, j.hhr_posn_code FROM boogoo_corehr.hhr_org_per_jobdata j "
                 "WHERE j.tenant_id = :b1 AND j.hhr_empid = :b2 AND j.hhr_emp_rcd = :b3 "
                 "AND :b4 BETWEEN j.hhr_efft_date AND j.hhr_efft_end_date")
        r = db.conn.execute(t, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=to_date).fetchone()
        if r is not None:
            return r['hhr_dept_code'], r['hhr_posn_code']
        else:
            return '', ''
