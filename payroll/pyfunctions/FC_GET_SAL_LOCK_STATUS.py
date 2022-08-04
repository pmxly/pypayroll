# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text


class PyFunction(FunctionObject):
    """
    Desc: 获取员工薪资锁定状态
    Author: David
    Date: 2021/07/05
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_SAL_LOCK_STATUS'
        self.country = 'CHN'
        self.desc = '获取员工薪资锁定状态'
        self.descENG = '获取员工薪资锁定状态'
        self.func_type = 'A'
        self.instructions = "获取员工薪资锁定状态"
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
        t = text("select hhr_acc_sal_lock from boogoo_payroll.hhr_py_acc_cntrl where tenant_id = :b1 and hhr_empid = :b2")
        r = db.conn.execute(t, b1=catalog.tenant_id, b2=catalog.emp_id).fetchone()
        if r is not None:
            sal_lock = r['hhr_acc_sal_lock']
            if sal_lock is None:
                return 'N'
            return sal_lock
        else:
            return 'N'
