from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject, ReturnObj
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 获取上一序号对应的薪资项目
    Author: David
    Date: 2019/10/21
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_PREV_PIN'
        self.country = 'CHN'
        self.desc = '获取上一序号对应的薪资项目函数'
        self.descENG = '获取上一序号对应的薪资项目函数'
        self.func_type = 'A'
        self.instructions = "获取上一序号对应的薪资项目，参数为薪资项目编码。返回薪资项目对象。"
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
                'PA': ['pin_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, pin_cd):
        ret_obj = ReturnObj()
        if not pin_cd:
            return ret_obj
        cur_catalog = gv.get_run_var_value('PY_CATALOG')
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        return_dic = get_db_pin_by_catalog(catalog=cur_catalog, seqnum=cur_catalog.prev_seq, pin_code_list=[pin_cd])
        ret_obj.amt = return_dic.get(pin_cd, 0)
        ret_obj.currency = vr_pg_currency
        return ret_obj
