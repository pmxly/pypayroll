# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text


class PyFunction(FunctionObject):
    """
    Desc: 获取入职原因
    Author: David
    Date: 2020/12/30
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_HIRE_REASON'
        self.country = 'CHN'
        self.desc = '获取入职原因'
        self.descENG = '获取入职原因'
        self.func_type = 'A'
        self.instructions = "获取入职原因。"
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': [],
                'VR': [],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        t = text("select j.hhr_action_reason_code from boogoo_corehr.hhr_org_per_jobdata j where j.tenant_id = :b1 and j.hhr_empid = :b2 "
                 "and j.hhr_job_indicator = 'P' and j.hhr_action_type_code = 'HIR'")
        r = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id).fetchone()
        if r is not None:
            return r['hhr_action_reason_code']
        else:
            return None
