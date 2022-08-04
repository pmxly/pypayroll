# coding:utf-8

from ..pyfunctions.function_object import FunctionObject


class PyFunction(FunctionObject):
    """
    Desc: 抛出异常函数
    Author: David
    Date: 2021/01/25
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_RAISE_EXCEPTION'
        self.country = 'CHN'
        self.desc = '抛出异常函数'
        self.descENG = '抛出异常函数'
        self.func_type = 'B'
        self.instructions = "抛出异常函数。"
        self.instructionsENG = self.instructions
        self.trace_dic = {}

    def func_exec(self, error_txt):
        raise Exception(error_txt)
