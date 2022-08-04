# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from datetime import date


class PyFunction(FunctionObject):
    """
    Desc: 获取员工所在公司函数
    Author: David
    Date: 2021/01/25
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_EMP_COMPANY'
        self.country = 'CHN'
        self.desc = '获取员工所在公司函数'
        self.descENG = '获取员工所在公司函数'
        self.func_type = 'A'
        self.instructions = "获取员工所在公司函数。输入参数：无; 输出参数：任职分段的一条记录"
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
                'PA': []
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self):
        """
        取配置维护“HHR.PY.POST.MAIN_COST_DATE”中的值作为日，以历经期结束日期所在年、月作为年、月，构造日期D1；
        以日期D1确定分段，并取任职分段表中对应分段的公司
        """

        prd_end_dt = gv.get_var_value('VR_F_PERIOD_END')
        year = prd_end_dt.year
        month = prd_end_dt.month
        main_cost_day = gv.get_run_var_value('MAIN_COST_DAY')
        d1 = date(year, month, main_cost_day)
        seg_info = gv.get_run_var_value('SEG_INFO_OBJ')

        items_dic = seg_info.seg_items_dic['*']
        seg_num = None
        for seg_item in items_dic.values():
            if seg_item.bgn_dt <= d1 <= seg_item.end_dt:
                seg_num = seg_item.segment_num
                break
        job_seg_dic = seg_info.job_seg_dic
        if seg_num in job_seg_dic:
            job_seg = job_seg_dic[seg_num]
            return job_seg
        else:
            # 如果d1对应的天数是15号，但是员工16号之后才入职的，取不到15号，就取第一个任职分段
            for job_seg in job_seg_dic.values():
                return job_seg
