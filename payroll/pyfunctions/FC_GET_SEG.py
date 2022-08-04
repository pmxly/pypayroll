# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log, add_fc_log_item


class PyFunction(FunctionObject):
    """
    Desc: 获取期间分段函数
    Author: David
    Date: 2019/07/24
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_GET_SEG'
        self.country = 'CHN'
        self.desc = '获取期间分段函数'
        self.descENG = '获取期间分段函数'
        self.func_type = 'B'
        self.instructions = "获取期间分段函数。传入分段号"
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
                'PA': ['seg_num']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, seg_num=0):
        seg_info = gv.get_run_var_value('SEG_INFO_OBJ')

        period_seg_dic = seg_info.period_seg_dic
        if seg_num == 0:
            max_seg_num = 0
            for seg_num_key in period_seg_dic.keys():
                max_seg_num = seg_num_key
            seg_num = max_seg_num

        if seg_num not in period_seg_dic:
            return

        period_seg = period_seg_dic[seg_num]
        gv.set_var_value('VR_SEG_BGN_DT', period_seg.seg_bgn_dt)        # 期间分段开始日期
        gv.set_var_value('VR_SEG_END_DT', period_seg.seg_end_dt)        # 期间分段结束日期
        gv.set_var_value('VR_CAL_DAYS', period_seg.cal_days)            # 期间分段日历日
        gv.set_var_value('VR_WORK_DAYS', period_seg.work_days)          # 期间分段工作日
        gv.set_var_value('VR_LEGAL_DAYS', period_seg.legal_days)        # 期间分段法定日
        gv.set_var_value('VR_CAL_S_DAYS', period_seg.cal_sum_days)      # 期间分段总日历日
        gv.set_var_value('VR_WORK_S_DAYS', period_seg.work_sum_days)    # 总工作日
        gv.set_var_value('VR_LEGAL_S_DAYS', period_seg.legal_sum_days)  # 总法定日

        if self.log_flag == 'Y':
            vr_lst = ['VR_SEG_BGN_DT', 'VR_SEG_END_DT', 'VR_CAL_DAYS', 'VR_WORK_DAYS',
                      'VR_LEGAL_DAYS', 'VR_CAL_S_DAYS', 'VR_WORK_S_DAYS', 'VR_LEGAL_S_DAYS']
            add_fc_log_item(self, 'VR', '', vr_lst)
