# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
import datetime


class PyFunction(FunctionObject):
    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_YT_CONSTANT_STD'
        self.country = 'CHN'
        self.desc = ''
        self.descENG = ''
        self.func_type = 'A'
        self.instructions = ""
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
                'PA': ['item_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, item_cd):
        if item_cd is None:
            item_cd = ''
        dict_key = "0_" + item_cd
        constant_std_data = gv.get_run_var_value('CONSTANT_STD_DATA')
        if dict_key not in constant_std_data:
            db = gv.get_db()
            t = text(
                "SELECT hhr_data_val1_dec FROM boogoo_payroll.hhr_py_bracket_data "
                "where tenant_id = 0 and hhr_bracket_cd = 'YT_CONSTANT_STD' "
                "and hhr_efft_date = '1990-01-01' and hhr_data_key1 = :b1 ")
            r = db.conn.execute(t, b1=item_cd).fetchone()
            if r is not None:
                constant_std_data[dict_key] = r['hhr_data_val1_dec']
                gv.set_var_value('YT_STD', r['hhr_data_val1_dec'])
        else:
            gv.set_var_value('YT_STD', constant_std_data[dict_key])