# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
from ...utils import add_months
from datetime import timedelta, datetime, date
from ..pysysutils.func_lib_02 import round_rule

one_day = timedelta(days=1)


class PyFunction(FunctionObject):
    """
    Desc: 获取前N个月工资函数
    Author: David
    Date: 2020/12/18
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_PREV_MONTHS_SAL'
        self.country = 'CHN'
        self.desc = '获取前N个月工资函数'
        self.descENG = '获取前N个月工资函数'
        self.func_type = 'A'
        self.instructions = "获取前N个月工资函数。输入参数：日期，月数，薪资项目编码。输出参数：平均金额，总月数"
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
                'PA': ['to_date', 'mon_count', 'pin_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, to_date, mon_count, pin_cd):
        """
        获取指定日期前N个月（N为指定的月数）的薪资结果中，指定的薪资项目平均金额；并返回实际月数。
        例如：指定日期为2020-02-15，指定月数为12，则取2019-02-15至2020-02-14的薪资结果，目录取数条件：
        hhr_empid=员工号
        hhr_emp_rcd=任职记录号
        hhr_f_cal_id = hhr_py_cal_id
        hhr_py_rec_stat<>'O'
        hhr_f_prd_bgn_dt<=结束日期（示例中为2020-02-14）
        hhr_f_prd_end_dt>=开始日期（示例中为2019-02-15）
        取到的目录中，序号最小的与序号最大的比较历经期结束日期，计算实际月数。未取到目录，总金额、总月数均为0。
        每个目录的薪资结果中取指定薪资项目的金额，累加后除以月数。
        """
        if isinstance(to_date, str):
            try:
                to_date = datetime.strptime(to_date, "%Y-%m-%d")
            except (TypeError, ValueError):
                try:
                    to_date = datetime.strptime(to_date, "%Y/%m/%d")
                except (TypeError, ValueError):
                    raise Exception("函数FC_GET_PREV_MONTHS_SAL的参数错误")
        elif not isinstance(to_date, date):
            raise Exception("函数FC_GET_PREV_MONTHS_SAL的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        begin_date = add_months(to_date, -mon_count)
        end_date = to_date - one_day
        t = text(
            "select c.hhr_seq_num, c.hhr_f_prd_end_dt, c.hhr_f_rt_cycle, r.hhr_amt from boogoo_payroll.hhr_py_cal_catalog c "
            "join boogoo_payroll.hhr_py_rslt_pin r on r.tenant_id = c.tenant_id and r.hhr_empid = c.hhr_empid and r.hhr_emp_rcd = c.hhr_emp_rcd "
            "and r.hhr_seq_num = c.hhr_seq_num and r.hhr_pin_cd = :b6 "
            "where c.tenant_id =:b1 "
            "and c.hhr_empid = :b2 and c.hhr_emp_rcd = :b3 and c.hhr_f_cal_id = c.hhr_py_cal_id and c.hhr_py_rec_stat <> 'O' "
            "and c.hhr_f_prd_bgn_dt <= :b4 and c.hhr_f_prd_end_dt >= :b5 order by c.hhr_seq_num asc ")
        rs = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id, b3=catalog.emp_rcd, b4=end_date, b5=begin_date,
                             b6=pin_cd).fetchall()
        total_amt = 0
        actual_months = 0
        for row in rs:
            # 只统计周期性的日历期间
            if row['hhr_f_rt_cycle'] == 'C':
                actual_months += 1
            total_amt += 0 if row['hhr_amt'] is None else row['hhr_amt']
        if actual_months != 0:
            avg_amt = round_rule('R01', total_amt / actual_months)
        else:
            avg_amt = 0
        return avg_amt, actual_months
