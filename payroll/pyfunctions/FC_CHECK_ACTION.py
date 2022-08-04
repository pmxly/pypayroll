# coding:utf-8

from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject
import datetime
from sqlalchemy.sql import text
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 人事事件检查函数
    Author: David
    Date: 2018/09/20
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_CHECK_ACTION'
        self.country = 'CHN'
        self.desc = '人事事件检查函数'
        self.descENG = '人事事件检查函数'
        self.func_type = 'A'
        self.instructions = '人事事件检查函数，参数1:操作编码，参数2:开始日期，参数3:结束日期。' \
                            '结果（1-代表匹配，0-代表不匹配）'
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
                'PA': ['action', 'bgn_dt', 'end_dt']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, action, bgn_dt, end_dt):
        """
        :param action: 操作编码
        :param bgn_dt: 开始日期
        :param end_dt: 结束日期
        :return: 1-代表匹配，0-代表不匹配, -1-代表错误
        """

        db = gv.get_db()

        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd

        if isinstance(bgn_dt, str) and isinstance(end_dt, str):
            try:
                bgn_dt = datetime.datetime.strptime(bgn_dt, "%Y-%m-%d")
                end_dt = datetime.datetime.strptime(end_dt, "%Y-%m-%d")
            except (TypeError, ValueError):
                try:
                    bgn_dt = datetime.datetime.strptime(bgn_dt, "%Y/%m/%d")
                    end_dt = datetime.datetime.strptime(end_dt, "%Y/%m/%d")
                except (TypeError, ValueError):
                    return -1
        elif not isinstance(bgn_dt, datetime.date) or not isinstance(end_dt, datetime.date):
            return -1

        sql = text("select 'y' from boogoo_corehr.hhr_org_per_jobdata a where a.tenant_id=:b1 and a.hhr_empid=:b2 "
                   "and a.hhr_emp_rcd=:b3 and a.hhr_efft_date between :b4 and :b5 and a.hhr_action_type_code=:b6")
        rs = db.conn.execute(sql, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=bgn_dt, b5=end_dt, b6=action).fetchone()
        if rs is not None:
            return 1
        else:
            return 0
