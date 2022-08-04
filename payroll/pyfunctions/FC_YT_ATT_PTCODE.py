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

        self.id = 'FC_YT_ATT_PTCODE'
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
                'PA': ['att_type', 'pt_code']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, att_type, pt_code):
        if att_type is None:
            att_type = ''
        if pt_code is None:
            pt_code = ''
        dict_key = "0_" + att_type + "_" + pt_code
        att_type_data = gv.get_run_var_value('ATT_TYPE_DATA')
        if dict_key not in att_type_data:
            db = gv.get_db()
            t = text(
                "SELECT hhr_data_val1 FROM boogoo_payroll.hhr_py_bracket_data where tenant_id = 0 and hhr_bracket_cd = 'YT_ATT_PTCODE' "
                "and hhr_efft_date = '1990-01-01' and hhr_data_key1 = :b1 and hhr_data_key2 = :b2")
            r = db.conn.execute(t, b1=att_type, b2=pt_code).fetchone()
            if r is not None:
                att_type_data[dict_key] = r['hhr_data_val1']
                gv.set_var_value('YT_YES_NO', r['hhr_data_val1'])
        else:
            gv.set_var_value('YT_YES_NO', att_type_data[dict_key])
