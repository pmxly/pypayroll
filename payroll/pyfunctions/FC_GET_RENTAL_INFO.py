# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
import datetime


class PyFunction(FunctionObject):
    """
    Desc: 获取租房信息函数
    Author: David
    Date: 2020/12/18
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_RENTAL_INFO'
        self.country = 'CHN'
        self.desc = '获取租房信息函数'
        self.descENG = '获取租房信息函数'
        self.func_type = 'A'
        self.instructions = "获取租房信息函数。输入参数：日期。"
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
                try:
                    to_date = datetime.datetime.strptime(to_date, "%Y/%m/%d")
                except (TypeError, ValueError):
                    raise Exception("函数FC_GET_RENTAL_INFO的参数错误")
        elif not isinstance(to_date, datetime.date):
            raise Exception("函数FC_GET_RENTAL_INFO的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        t = text("select hhr_rental_info from boogoo_payroll.hhr_py_rental_info where tenant_id = :b1 and hhr_empid = :b2 "
                 "and :b3 between hhr_efft_date and hhr_efft_end_date")
        r = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id, b3=to_date).fetchone()
        if r is not None:
            return r['hhr_rental_info']
        else:
            return None
