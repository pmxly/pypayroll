# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
from datetime import datetime, date


class PyFunction(FunctionObject):
    """
    Desc: 获取派遣公司
    Author: David
    Date: 2020/12/30
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_DISP_COMPANY'
        self.country = 'CHN'
        self.desc = '获取派遣公司'
        self.descENG = '获取派遣公司'
        self.func_type = 'A'
        self.instructions = "获取派遣公司。输入参数：日期。输出参数：派遣公司。"
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
                to_date = datetime.strptime(to_date, "%Y-%m-%d")
            except (TypeError, ValueError):
                try:
                    to_date = datetime.strptime(to_date, "%Y/%m/%d")
                except (TypeError, ValueError):
                    raise Exception("函数FC_GET_DISP_COMPANY的参数错误")
        elif not isinstance(to_date, date):
            raise Exception("函数FC_GET_DISP_COMPANY的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        t = text(
            "select hhr_contract_subject from boogoo_corehr.hhr_org_per_contract_dis where tenant_id = :b1 and hhr_empid = :b2 "
            "and :b3 between hhr_begin_date and hhr_estimate_end_date order by hhr_begin_date desc")
        r = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id, b3=to_date).fetchone()
        if r is not None:
            return r['hhr_contract_subject']
        else:
            return None
