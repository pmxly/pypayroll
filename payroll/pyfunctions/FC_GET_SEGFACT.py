# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log, add_fc_log_item


class PyFunction(FunctionObject):
    """
    Desc: 获取分段因子函数
    Author: David
    Date: 2019/07/24
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_GET_SEGFACT'
        self.country = 'CHN'
        self.desc = '获取分段因子函数'
        self.descENG = '获取分段因子函数'
        self.func_type = 'B'
        self.instructions = "获取分段因子函数。传入分段号、分段规则。"
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
                'PA': ['seg_rule_cd', 'seg_num']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, seg_rule_cd, seg_num=0):
        seg_info = gv.get_run_var_value('SEG_INFO_OBJ')

        seg_fact_dic = seg_info.seg_fact_dic
        period_seg_dic = seg_info.period_seg_dic
        if seg_num == 0:
            max_seg_num = 0
            for seg_num_key in period_seg_dic.keys():
                max_seg_num = seg_num_key
            seg_num = max_seg_num

        if seg_num not in period_seg_dic:
            return
        if seg_rule_cd not in seg_fact_dic:
            return

        seg_fact_lst = seg_fact_dic[seg_rule_cd]

        seg_fact = None
        for seg_fact in seg_fact_lst:
            if seg_fact.segment_num == seg_num:
                break
        if seg_fact is not None:
            gv.set_var_value('VR_NUMER_VAL', seg_fact.numerator_val)                # (分段因子)分子
            gv.set_var_value('VR_DENOM_VAL', seg_fact.denominator_val)              # (分段因子)分母
            gv.set_var_value('VR_FACTOR_VAL', seg_fact.factor_val * 100000)         # (分段因子)因子值, 赋值前先扩大100000倍

            if self.log_flag == 'Y':
                vr_lst = ['VR_NUMER_VAL', 'VR_DENOM_VAL', 'VR_FACTOR_VAL']
                add_fc_log_item(self, 'VR', '', vr_lst)
