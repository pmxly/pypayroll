# coding:utf-8

from ..pyexecute.pycalculate.seg.prev_seg_info import PrevSegInfo
from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from ..pysysutils.func_lib_02 import get_prd_date_lst


class PyFunction(FunctionObject):
    """
    Desc: 获取上月期间分段函数
    Author: David
    Date: 2021/01/28
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_PREV_PERIOD_SEG'
        self.country = 'CHN'
        self.desc = '获取上月期间分段函数'
        self.descENG = '获取上月期间分段函数'
        self.func_type = 'B'
        self.instructions = "获取上月期间分段函数。"
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
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        """
        逻辑：根据当前历经期（期间编码、年度、序号）找到上一期间年度、上一期间序号；例如：根据Y01、2021、3找到2021、2；
             再根据期间编码、上一期间年度、上一期间序号，找到对应的开始日期、结束日期；例如：根据Y01、2021、2找到2021-02-01、2021-02-28；
             构造此员工任职在上月的期间分段对象。数据结构及取数逻辑同历经期的期间分段对象，仅时间范围是上月。
        """
        cur_cal_obj = gv.get_run_var_value('CUR_CAL_OBJ')
        period_cd = cur_cal_obj.period_id
        period_year = cur_cal_obj.period_year
        period_num = cur_cal_obj.period_num
        prd_info_lst1 = get_prd_date_lst(period_cd, period_year, period_num)
        prd_last_year = prd_info_lst1[2]
        last_prd_num = prd_info_lst1[3]
        prd_info_lst2 = get_prd_date_lst(period_cd, prd_last_year, last_prd_num)
        prev_prd_start_dt = prd_info_lst2[0]
        prev_prd_end_dt = prd_info_lst2[1]
        gv.set_run_var_value('PREV_SEG_INFO_OBJ', PrevSegInfo(prev_prd_start_dt, prev_prd_end_dt))
