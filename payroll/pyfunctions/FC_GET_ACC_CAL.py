# coding:utf-8

from ..pyfunctions.function_object import FunctionObject, ReturnObj
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log


class PyFunction(FunctionObject):
    """
    Desc: 获取历经期累计日历
    Author: David
    Date: 2019/10/21
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_GET_ACC_CAL'
        self.country = 'CHN'
        self.desc = '获取历经期累计日历函数'
        self.descENG = '获取历经期累计日历函数'
        self.func_type = 'A'
        self.instructions = '获取历经期累计日历，参数为累计类型，返回累计日历对象。'
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
                'PA': ['acc_type']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, acc_type):
        ret_obj = ReturnObj()
        catalog = gv.get_run_var_value('PY_CATALOG')
        period_cd = catalog.f_period_cd
        period_year = catalog.f_period_year
        period_num = catalog.f_prd_num

        acc_key = str(period_cd) + str(period_year) + str(period_num)
        acc_cal_dic = gv.get_run_var_value("ACC_CAL_DIC")
        if acc_key in acc_cal_dic:
            acc_cal_lst = acc_cal_dic[acc_key]
        else:
            return ret_obj

        for acc_cal in acc_cal_lst:
            add_type = acc_cal.add_type
            if add_type == acc_type:
                ret_obj.acc_year = acc_cal.add_year
                ret_obj.acc_num = acc_cal.add_num
                return ret_obj

        return ret_obj
