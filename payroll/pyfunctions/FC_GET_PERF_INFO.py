# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text


class PyFunction(FunctionObject):
    """
    Desc: 获取绩效信息函数
    Author: David
    Date: 2020/12/18
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_PERF_INFO'
        self.country = 'CHN'
        self.desc = '获取绩效信息函数'
        self.descENG = '获取绩效信息函数'
        self.func_type = 'A'
        self.instructions = "获取绩效信息函数。输入参数：评价类型、年度、期间；输出参数：分数，绩效等级"
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
                'PA': ['eval_type', 'year', 'period']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, eval_type, year, period):
        if not eval_type or not year or not period:
            raise Exception("函数FC_GET_PERF_INFO的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        t = text(
            "select hhr_perf_score, hhr_perf_grade, hhr_perf_salary_score, hhr_perf_add_score, hhr_perf_reduce_score from boogoo_corehr.hhr_org_performance_rstl where tenant_id = :b1 and hhr_empid = :b2 and hhr_evaluation_type = :b3 and hhr_year = :b4 and hhr_period = :b5")
        r = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id, b3=eval_type, b4=year, b5=period).fetchone()
        if r is not None:
            return r['hhr_perf_score'], r['hhr_perf_grade'], r['hhr_perf_salary_score'], r['hhr_perf_add_score'], r['hhr_perf_reduce_score']
        else:
            return 0, None, 0, 0, 0
