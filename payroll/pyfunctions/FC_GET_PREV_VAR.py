from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject
from ..pysysutils.func_lib_02 import get_db_var_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 获取上一序号对应的变量
    Author: David
    Date: 2019/10/21
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_PREV_VAR'
        self.country = 'CHN'
        self.desc = '获取上一序号对应的变量函数'
        self.descENG = '获取上一序号对应的变量函数'
        self.func_type = 'A'
        self.instructions = "获取上一序号对应的变量，参数为变量编码。返回变量值。"
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
                'PA': ['var_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, var_cd):
        if not var_cd:
            return None
        cur_catalog = gv.get_run_var_value('PY_CATALOG')
        return_dic = get_db_var_by_catalog(catalog=cur_catalog, seqnum=cur_catalog.prev_seq, var_code_list=[var_cd])
        var_val = return_dic.get(var_cd, None)
        return var_val
