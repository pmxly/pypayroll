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

        self.id = 'FC_YT_ABS_PTCODE'
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
                'PA': ['pt_code']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, pt_code):
        if pt_code is None:
            pt_code = ''
        dict_key = "0_" + pt_code
        abs_ptcode_data = gv.get_run_var_value('ABS_PTCODE_DATA')
        if dict_key not in abs_ptcode_data:
            db = gv.get_db()
            t = text(
                "SELECT hhr_data_val1, hhr_data_val2, hhr_data_val3, hhr_data_val4, hhr_data_val5 "
                "FROM boogoo_payroll.hhr_py_bracket_data where tenant_id = 0 and hhr_bracket_cd = 'YT_ABS_PTCODE' "
                "and hhr_efft_date = '1990-01-01' and hhr_data_key1 = :b1 ")
            r = db.conn.execute(t, b1=pt_code).fetchone()
            if r is not None:
                abs_ptcode_data[dict_key] = (r['hhr_data_val1'], r['hhr_data_val2'], r['hhr_data_val3'], r[
                    'hhr_data_val4'], r['hhr_data_val5'])
                gv.set_var_value('YT_ABS_MEAL', r['hhr_data_val1'])
                gv.set_var_value('YT_ABS_FULL', r['hhr_data_val2'])
                gv.set_var_value('YT_NOTE', r['hhr_data_val3'])
                gv.set_var_value('YT_ABS_BUSS', r['hhr_data_val4'])
                gv.set_var_value('YT_ABS_ABSENT', r['hhr_data_val5'])
        else:
            gv.set_var_value('YT_ABS_MEAL', abs_ptcode_data[dict_key][0])
            gv.set_var_value('YT_ABS_FULL', abs_ptcode_data[dict_key][1])
            gv.set_var_value('YT_NOTE', abs_ptcode_data[dict_key][2])
            gv.set_var_value('YT_ABS_BUSS', abs_ptcode_data[dict_key][3])
            gv.set_var_value('YT_ABS_ABSENT', abs_ptcode_data[dict_key][4])
