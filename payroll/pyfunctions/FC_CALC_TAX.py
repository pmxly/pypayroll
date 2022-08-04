# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pyexecute.pycalculate.tax.tax_process import TaxProcess
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 个税处理
    Author: David
    Date: 2018/10/11
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_CALC_TAX'
        self.country = 'CHN'
        self.desc = '个税处理函数'
        self.descENG = '个税处理函数'
        self.func_type = 'B'
        self.instructions = "个税处理函数"
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': ['WT_TAXBASE01_EE', 'WT_TAXBASE01_ER', 'WT_TAXBASE02_EE', 'WT_TAXBASE02_ER', 'WT_TAXBASE03_EE', 'WT_TAXBASE03_ER',
                       'WT_TAXBASE04_EE', 'WT_TAXBASE04_ER', 'WT_TAXBASE05_EE', 'WT_TAXBASE05_ER'],
                'VR': ['VR_TAXTYPE', 'VR_TAXFREE', 'VR_TAXRATE01_EE', 'VR_TAXDEDUC01_EE',
                       'VR_TAXRATE01_ER', 'VR_TAXDEDUC01_ER', 'VR_TAXRATE02_EE', 'VR_TAXDEDUC02_EE',
                       'VR_TAXRATE02_ER', 'VR_TAXDEDUC02_ER', 'VR_TAXRATE03_EE', 'VR_TAXDEDUC03_EE', 'VR_TAXRATE03_ER', 'VR_TAXDEDUC03_ER',
                       'VR_TAXRATE04', 'VR_PG_CURRENCY', 'VR_TAXAREA', 'VR_F_PERIOD_YEAR', 'VR_COMPYEAR', 'VR_TAX_ZERO'],
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        # 按历经期处理个税
        catalog = gv.get_run_var_value('PY_CATALOG')
        cur_cal = gv.get_run_var_value('CUR_CAL_OBJ')
        tax_process_obj = TaxProcess(fc_obj=self, catalog=catalog)
        tax_process_obj.calc_tax()
