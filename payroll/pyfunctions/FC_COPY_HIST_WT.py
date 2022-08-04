from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 复制历史序号薪资项目函数
    Author: David
    Date: 2019/06/09
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG', 'src_pin_cd', 'tgt_pin_cd']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_COPY_HIST_WT'
        self.country = 'CHN'
        self.desc = '复制历史序号薪资项目函数'
        self.descENG = '复制历史序号薪资项目函数'
        self.func_type = 'B'
        self.instructions = "复制历史序号薪资项目函数。参数1为薪资项目编码1，参数2为薪资项目编码2。"
        self.instructionsENG = self.instructions

        self.src_pin_cd = ''
        self.tgt_pin_cd = ''

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
                'PA': ['src_pin_cd', 'tgt_pin_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, src_pin_cd, tgt_pin_cd):
        catalog = gv.get_run_var_value('PY_CATALOG')
        cur_type = gv.get_run_var_value('CUR_CALCULATE_TYPE')
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        # 被追溯的期间（期间状态R）：取历史序号对应的期间的薪资结果（输入参数1指定的薪资项目），赋值给本次薪资计算（输入参数2指定的薪资项目）。
        if cur_type == 'R':
            pin_list = [src_pin_cd]
            result_dic = get_db_pin_by_catalog(catalog=catalog, seqnum=catalog.hist_seq, pin_code_list=pin_list)
            gv.get_pin(tgt_pin_cd).segment['*'].amt = result_dic.get(src_pin_cd, 0)
            gv.get_pin(tgt_pin_cd).segment['*'].currency = vr_pg_currency
