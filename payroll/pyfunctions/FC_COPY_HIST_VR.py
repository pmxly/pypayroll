from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject
from ..pysysutils.func_lib_02 import get_db_var_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 复制历史序号变量函数
    Author: David
    Date: 2021/03/17
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG', 'src_pin_cd', 'tgt_pin_cd']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_COPY_HIST_VR'
        self.country = 'CHN'
        self.desc = '复制历史序号变量函数'
        self.descENG = '复制历史序号变量函数'
        self.func_type = 'B'
        self.instructions = "复制历史序号变量函数"
        self.instructionsENG = self.instructions

        self.src_var_cd = ''
        self.tgt_var_cd = ''

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': [self.tgt_pin_cd],
                'VR': [],
                'PA': ['src_var_cd', 'tgt_var_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, src_var_cd, tgt_var_cd):
        catalog = gv.get_run_var_value('PY_CATALOG')
        cur_type = gv.get_run_var_value('CUR_CALCULATE_TYPE')

        # 被追溯的期间（期间状态R）：取历史序号对应的期间的变量结果（输入参数1指定的变量），赋值给本次薪资计算（输入参数2指定的变量）。
        if cur_type == 'R':
            var_list = [src_var_cd]
            result_dic = get_db_var_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, var_code_list=var_list)
            var_val = result_dic.get(src_var_cd, None)
            gv.get_var_obj(tgt_var_cd).value = var_val
