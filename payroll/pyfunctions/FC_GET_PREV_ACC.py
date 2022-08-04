from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject, ReturnObj
from ..pysysutils.func_lib_02 import get_db_acc_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 获取上一序号对应的累计项目
    Author: David
    Date: 2019/10/21
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_PREV_ACC'
        self.country = 'CHN'
        self.desc = '获取上一序号对应的累计项目函数'
        self.descENG = '获取上一序号对应的累计项目函数'
        self.func_type = 'A'
        self.instructions = "获取上一序号对应的累计项目，参数为累计项目编码。返回累计项目对象。"
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
                'PA': ['acc_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, acc_cd):
        ret_obj = ReturnObj()
        if not acc_cd:
            return ret_obj
        cur_catalog = gv.get_run_var_value('PY_CATALOG')
        return_dic = get_db_acc_by_catalog(catalog=cur_catalog, seqnum=cur_catalog.prev_seq, acc_code_list=[acc_cd])
        if acc_cd in return_dic:
            ret_lst = return_dic.get(acc_cd)
            ret_obj.acc_type = ret_lst[0]
            ret_obj.acc_year = ret_lst[1]
            ret_obj.acc_num = ret_lst[2]
            ret_obj.amt = ret_lst[3]
            ret_obj.currency = ret_lst[4]
        return ret_obj
