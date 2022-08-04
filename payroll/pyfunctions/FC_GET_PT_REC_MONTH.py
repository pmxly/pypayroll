# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from ..pysysutils.func_lib_02 import get_acc_year_seq
from sqlalchemy import text
from ..pyexecute.pycalculate.table.pt_emp_mon_dtl import PtEmpMonDtl
from ..pyexecute.pycalculate.table.rslt_abs import Abs


class PyFunction(FunctionObject):
    """
    Desc: 获取月度考勤结果
    Author: David
    Date: 2020/12/30
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_PT_REC_MONTH'
        self.country = 'CHN'
        self.desc = '获取月度考勤结果'
        self.descENG = '获取月度考勤结果'
        self.func_type = 'A'
        self.instructions = "获取月度考勤结果。"
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
                'PA': ['month_flag']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, month_flag):
        """
        从考勤表hhr_pt_custom_emp_monthly_dtl中取值；
        当月标识含L，需获取上月考勤结果，当月标识含C，需获取本月考勤结果；
        根据历经期对应的月度累计（M）的累计年度、累计序号确定本月的年份、月份；再确定上月的年份、月份。
        根据年份、月份从考勤表中取值。
        """

        if month_flag is None or month_flag == '':
            raise Exception("函数FC_GET_PT_REC_MONTH的参数错误")

        acc_year_seq = get_acc_year_seq('M')
        cur_year = acc_year_seq[0]
        cur_month = acc_year_seq[1]

        if not cur_year or not cur_month:
            return

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        t = text(
            "SELECT tenant_id, hhr_year, hhr_month, hhr_pt_code, hhr_belong_year, hhr_belong_month, hhr_empid, hhr_amount "
            "FROM boogoo_attendance.hhr_pt_custom_emp_monthly_dtl "
            "WHERE tenant_id = :b1 AND hhr_empid = :b2 AND hhr_year = :b3 AND hhr_month = :b4")
        result = []
        if 'L' in month_flag:
            if cur_month == 1:
                year = cur_year - 1
                last_month = 12
            else:
                year = cur_year
                last_month = cur_month - 1
            rs = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id, b3=year, b4=last_month).fetchall()
            for r in rs:
                result.append(PtEmpMonDtl(**r))
        if 'C' in month_flag:
            rs = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id, b3=cur_year, b4=cur_month).fetchall()
            for r in rs:
                result.append(PtEmpMonDtl(**r))

        for d in result:
            v_dic = {'tenant_id': d.tenant_id, 'emp_id': d.empid, 'emp_rcd': catalog.emp_rcd,
                     'seq_num': catalog.seq_num, 'abs_cd': d.pt_code,
                     'quantity': d.amount, 'year': d.year, 'month': d.month, 'belong_year': d.belong_year,
                     'belong_month': d.belong_month}
            Abs(**v_dic).insert()

        return result
