# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
from datetime import datetime, date
from ..pyexecute.pycalculate.table.posn import Posn


class PyFunction(FunctionObject):
    """
    Desc: 获取岗位信息函数
    Author: David
    Date: 2020/12/18
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_POSN_INFO'
        self.country = 'CHN'
        self.desc = '获取岗位信息函数'
        self.descENG = '获取岗位信息函数'
        self.func_type = 'A'
        self.instructions = "获取岗位信息函数。输入参数：岗位、日期；输出参数：岗位信息对象（所有字段）"
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
                'PA': ['posn_cd', 'to_date']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, posn_cd, to_date=None):
        if posn_cd is None or posn_cd == '':
            raise Exception("函数FC_GET_POSN_INFO的参数错误")

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
                    raise Exception("函数FC_GET_POSN_INFO的参数错误")
        elif not isinstance(to_date, date):
            raise Exception("函数FC_GET_POSN_INFO的参数错误")

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        t = text(
            "select * from boogoo_corehr.hhr_org_posn where tenant_id = :b1 and hhr_posn_code = :b2 and :b3 BETWEEN hhr_efft_date and hhr_efft_end_date")
        r = db.conn.execute(t, b1=catalog.tenant_id, b2=posn_cd, b3=to_date).fetchone()
        if r is not None:
            posn_info = Posn(**r)
        else:
            posn_info = Posn(**{})
        return posn_info
