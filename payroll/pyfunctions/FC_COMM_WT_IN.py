from ..pysysutils import global_variables as gv
from ..pyfunctions.function_object import FunctionObject
from ..pysysutils.func_lib_02 import get_db_pin_by_catalog
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 通用流入处理函数
    Author: David
    Date: 2019/06/09
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG', 'src_pin_cd', 'tgt_pin_cd']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_COMM_WT_IN'
        self.country = 'CHN'
        self.desc = '通用流入处理函数'
        self.descENG = '通用流入处理函数'
        self.func_type = 'B'
        self.instructions = "通用流入处理函数。参数1为薪资项目编码1，参数2为薪资项目编码2。"
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
                'PA': ['src_pin_cd', 'tgt_pin_cd', 'tuple_data_list']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, src_pin_cd, tgt_pin_cd, tuple_data_list=None):
        cur_type = gv.get_run_var_value('CUR_CALCULATE_TYPE')
        retro_catalog_list = gv.get_run_var_value('RETRO_CATALOG_LIST')
        vr_pg_currency = gv.get_var_value('VR_PG_CURRENCY')  # 历经期薪资组货币

        # 被追溯的期间（期间状态R）：无流入，即不需要进行处理
        # 当前期间（期间状态N）或 更正期间（期间状态C）：本次计算的被追溯期间薪资项目（输入参数1指定的薪资项目）总和，流入另一薪资项目（输入参数2指定的薪资项目）。
        # 货币=历经期薪资组货币：VR_PG_CURRENCY
        if cur_type == 'N' or cur_type == 'C':
            if tuple_data_list is None:
                src_wt_total_out = 0
                if retro_catalog_list is not None:
                    pin_list = [src_pin_cd]
                    for retro_catalog in retro_catalog_list:
                        result_dic = get_db_pin_by_catalog(catalog=retro_catalog, seqnum=retro_catalog.seq_num,
                                                           pin_code_list=pin_list)
                        src_wt_total_out += result_dic.get(src_pin_cd, 0)

                gv.get_pin(tgt_pin_cd).segment['*'].amt = src_wt_total_out
                # 赋值历经期薪资组货币
                gv.get_pin(tgt_pin_cd).segment['*'].currency = vr_pg_currency
            else:
                if retro_catalog_list is None:
                    retro_catalog_list = []
                pin_list = []
                for data in tuple_data_list:
                    src_pin_cd = data[0]
                    pin_list.append(src_pin_cd)

                retro_result_dic = {}
                for retro_catalog in retro_catalog_list:
                    result_dic = get_db_pin_by_catalog(catalog=retro_catalog, seqnum=retro_catalog.seq_num,
                                                       pin_code_list=pin_list)
                    retro_result_dic[id(retro_catalog)] = result_dic

                for data in tuple_data_list:
                    src_pin_cd = data[0]
                    tgt_pin_cd = data[1]
                    src_wt_total_out = 0
                    for retro_catalog in retro_catalog_list:
                        result_dic = retro_result_dic[id(retro_catalog)]
                        src_wt_total_out += result_dic.get(src_pin_cd, 0)

                    gv.get_pin(tgt_pin_cd).segment['*'].amt = src_wt_total_out
                    # 赋值历经期薪资组货币
                    gv.get_pin(tgt_pin_cd).segment['*'].currency = vr_pg_currency