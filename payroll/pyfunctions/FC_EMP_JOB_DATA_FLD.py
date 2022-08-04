# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
import datetime


class PyFunction(FunctionObject):
    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_EMP_JOB_DATA_FLD.py'
        self.country = 'CHN'
        self.desc = ''
        self.descENG = ''
        self.func_type = 'A'
        self.instructions = ""
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
    def func_exec(self, field, to_date=None):
        if to_date is None:
            prd_end_dt = gv.get_var_value('VR_F_PERIOD_END')
            to_date = prd_end_dt

        if isinstance(to_date, str):
            try:
                to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")
            except (TypeError, ValueError):
                to_date = datetime.datetime.strptime(to_date, "%Y/%m/%d")
        elif not isinstance(to_date, datetime.date):
            raise Exception("函数FC_EMP_JOB_DATA_FLD的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd

        sql_t = 'SELECT j.' + field + ' FROM boogoo_corehr.hhr_org_per_jobdata j ' \
                                      'WHERE j.tenant_id = :b1 AND j.hhr_empid = :b2 AND j.hhr_emp_rcd = :b3  ' \
                                      'AND :b4 BETWEEN j.hhr_efft_date AND j.hhr_efft_end_date'
        t = text(sql_t)
        r = db.conn.execute(t, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=to_date).fetchone()
        if r is not None:
            return r[field]
        else:
            return None
