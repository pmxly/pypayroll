# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text


class PyFunction(FunctionObject):
    """
    Desc: 获取工伤开始日期函数
    Author: David
    Date: 2020/12/18
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_OCCUP_INJURY_BGN_DT'
        self.country = 'CHN'
        self.desc = '获取工伤开始日期函数'
        self.descENG = '获取工伤开始日期函数'
        self.func_type = 'A'
        self.instructions = "获取工伤开始日期函数。输入参数：无。输出参数：工伤开始日期"
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
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        # 从工伤记录中取值，历经期开始日期前，已认定上工伤的记录中，发生工伤时间最近的一次
        f_prd_bgn_dt = catalog.f_prd_bgn_dt
        t = text(
            "SELECT FD_ACCIDENT_TIME FROM boogoo_corehr.T_INDUSTRIAL_PROCESS WHERE FD_STAFF_NUMBER = :b1 AND FD_ACCIDENT_TIME < :b2 "
            "AND FD_RDJL = '1' ORDER BY FD_ACCIDENT_TIME DESC")
        r = db.conn.execute(t, b1=catalog.emp_id, b2=f_prd_bgn_dt).fetchone()
        if r is not None:
            return r['FD_ACCIDENT_TIME']
        else:
            return None
