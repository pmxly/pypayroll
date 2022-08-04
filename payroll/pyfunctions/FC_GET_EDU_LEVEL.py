# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text


class PyFunction(FunctionObject):
    """
    Desc: 获取学历
    Author: David
    Date: 2020/12/30
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_EDU_LEVEL'
        self.country = 'CHN'
        self.desc = '获取学历'
        self.descENG = '获取学历'
        self.func_type = 'A'
        self.instructions = "获取学历。输入参数：A：第一学历，B：最高学历。输出参数：学历。"
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
                'PA': ['option']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, option):
        if option not in ['A', 'B']:
            raise Exception("函数FC_GET_EDU_LEVEL的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        s = "select hhr_edu from boogoo_corehr.hhr_org_per_edu_exp where tenant_id = :b1 and hhr_empid = :b2 "
        if option == 'A':
            s = s + " and hhr_first_edu = 'Y'"
        elif option == 'B':
            s = s + " and hhr_high_edu = 'Y'"
        r = db.conn.execute(text(s), b1=catalog.tenant_id, b2=catalog.emp_id).fetchone()
        if r is not None:
            return r['hhr_edu']
        else:
            return None
