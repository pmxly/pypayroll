# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
from ...utils import add_months


class PyFunction(FunctionObject):
    """
    Desc: 获取产假信息函数
    Author: David
    Date: 2020/12/18
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_MATERNITY_LEAVE'
        self.country = 'CHN'
        self.desc = '获取产假信息函数'
        self.descENG = '获取产假信息函数'
        self.func_type = 'A'
        self.instructions = "获取产假信息函数。输入参数：无。输出参数：产假开始日期、产假时长"
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
        # 从请假记录中取值，历经期开始日期前18个月的产假记录中，取最小的开始日期；累计期间内产假总时长
        f_prd_bgn_dt = catalog.f_prd_bgn_dt
        begin_date = add_months(f_prd_bgn_dt, -18)
        t = text(
            "select hhr_start_dt, hhr_hour_amount from boogoo_attendance.hhr_pt_emp_leave_rec where tenant_id = :b1 and hhr_empid = :b2 "
            "and hhr_start_dt BETWEEN :b3 and :b4 and hhr_pt_code = :b5 order by hhr_start_dt asc")
        rs = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id, b3=begin_date, b4=f_prd_bgn_dt, b5='0005').fetchall()
        i = 0
        start_dt = None
        hour_amt = 0
        for row in rs:
            if i == 0:
                start_dt = row['hhr_start_dt']
            hour_amt += (0 if row['hhr_hour_amount'] is None else row['hhr_hour_amount'])
            i += 1
        return start_dt, hour_amt
