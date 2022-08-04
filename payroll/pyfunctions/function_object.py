# coding:utf-8
# from abc import ABCMeta
# from abc import abstractmethod


class FunctionObject:
    """
    函数的基类
    create by wangling 2018/6/27
    """

    def __init__(self):
        # 函数唯一id
        self.id = ''
        # 函数类型 A：有返回值  B:没有返回值
        self.func_type = 'A'
        # 函数所属国家ALL:所有国家 CHN:中国
        self.country = 'ALL'
        # 函数描述，用于系统中的描述
        self.desc = ''
        # 函数英文描述，用于系统中的描述
        self.descENG = ''
        # 函数使用中文说明
        self.instructions = ''
        # 函数使用英文说明
        self.instructionsENG = ''
        # 选中函数后默认带出来的函数文本，用户可以在此基础上修改
        # self.edit_text = self.id+'()'


class ReturnObj:

    def __init__(self):
        self.acc_type = ''
        self.acc_year = 1900
        self.acc_num = 0

        self.amt = 0
        self.currency = ''

        self.avg_anl_sal = 0
        self.avg_mon_sal = 0
        self.min_mon_sal = 0
