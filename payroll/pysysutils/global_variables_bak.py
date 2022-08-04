# coding:utf-8
"""
全局变量控制，所有全局变量在这里set，get，clean，在模块（pyfunctions.init_sys_variables）中初始化
create by wangling 2018/6/27
"""

from ...dbengine import DataBaseAlchemy


class DevVariableObject:
    """
    薪资计算过程中的变量对象
    create by wangling 2018/8/7
    """

    def __init__(self, vr_id, description, value):
        self.id = vr_id
        self.description = description
        self.value = value


def init_flask():
    # Flask端事务处理数据库连接
    global flask_db
    flask_db = DataBaseAlchemy()


def get_flask_db():
    return flask_db


# 初始化存放变量的字段和存放只读变量的字典
def init():
    # 全局变量的字段，贯穿整个计算工程，变量为用户在系统中定义的和系统标准的VariableObject对象
    global variable_dic
    # 薪资计算过程时，代码需要用到的变量（用于运行过程中各个步骤之前的变量传递）
    global dev_variable_dic
    # 薪资项目字典，包含针对当前员工历经期所有有效的薪资项目
    global pin_dic
    # 薪资项目累加器字典
    global pin_accumulate_dic
    # 事务处理数据库连接
    global db
    # 实时更新库连接
    global real_time_db

    db = DataBaseAlchemy()
    real_time_db = DataBaseAlchemy()
    variable_dic = {}
    dev_variable_dic = {}
    pin_dic = {}
    pin_accumulate_dic = {}


# 获取事务处理数据库链接
def get_db():
    return db


# 获取实时处理数据库链接
def get_real_time_db():
    return real_time_db


# 插入系统VariableObject变量
def set_val_object(key, variable_object):
    variable_dic[key] = variable_object


# 新建/修改VariableObject变量
def set_var_value(key, value):
    if key in variable_dic:
        variable_dic[key].value = value


# 获取VariableObject变量值
def get_var_value(key, def_value=None):
    try:
        return variable_dic[key].value
    except KeyError:
        # raise Exception(key + ' does not exist')
        return def_value


# 获取VariableObject变量对象
def get_var_obj(key):
    try:
        return variable_dic[key]
    except KeyError:
        return None


# 判断VariableObject变量是否已经存在
def is_var_exists(key):
    return key in variable_dic


# 获取VariableObject全局变量字典
def get_variable_dic():
    return variable_dic


# 插入系统DevVariableObject变量
def set_run_var_obj(key, description, value):
    """
    插入DevVariableObject变量,此类变量在算薪过程中作为临时变量使用
    :param key:唯一变量名
    :param description:描述
    :param value:值，可以是对象
    :return:
    """
    dev_variable_dic[key] = DevVariableObject(key, description, value)


# 设置运行过程中的变量，变量可以是常量或者对象
def set_run_var_value(key, value):
    """
    设置运行过程中的变量，变量可以是常量或者对象
    :param key:变量名
    :param value:值
    :return:
    """
    if key.strip() != '':
        if key in dev_variable_dic:
            dev_variable_dic[key].value = value


# 设置运行过程中的变量，变量可以是常量或者对象
def get_run_var_value(key, def_value=None):
    """
    获取DevVariableObject变量值
    :param key:变量名
    :param def_value:默认值
    :return:
    """
    try:
        return dev_variable_dic[key].value
    except (KeyError, NameError):
        return def_value


# 设置运行过程中的变量对象，变量可以是常量或者对象
def get_run_var_obj(key):
    """
    获取DevVariableObject变量对象
    :param key:变量名
    :return:
    """
    try:
        return dev_variable_dic[key]
    except KeyError:
        return None


# 判断DevVariableObject变量是否已经存在
def is_run_var_exists(key):
    return key in dev_variable_dic


# 获取运行过程全局变量字典
def get_run_variable_dic():
    return dev_variable_dic


# 获取薪资项目对象
def get_pin(pin_code):
    if pin_code in pin_dic:
        return pin_dic[pin_code]
    # else:
    #     raise Exception(pin_code + ' does not exist')


# 获取薪资项目累计对象
def get_pin_acc(acc_code):
    if acc_code in pin_accumulate_dic:
        return pin_accumulate_dic[acc_code]
    # else:
    #     raise Exception(acc_code + ' does not exist')


# 插入薪资项目
def set_pin(pin_obj):
    if pin_obj is not None:
        pin_dic[pin_obj.pin_id] = pin_obj


# 插入薪资项目累计对象
def set_pin_acc(acc_obj):
    if acc_obj is not None:
        pin_accumulate_dic[acc_obj.acc_code] = acc_obj


# 获取薪资项目
def get_pin_dic():
    return pin_dic


# 获取薪资项目累计字典
def get_pin_acc_dic():
    return pin_accumulate_dic


# 判断薪资项目是否在pin_dic字典中
def pin_in_dic(pin_id):
    if pin_id in pin_dic:
        return True
    else:
        return False


# 判断薪资项目累计是否在pin_accumulate_dic字典中
def pin_acc_in_dic(acc_code):
    if acc_code in pin_accumulate_dic:
        return True
    else:
        return False


# 清除薪资项目字典
def clear_pin_dic():
    global pin_dic
    pin_dic = {}


# 清楚薪资项目累计字典
def clear_pin_acc_dic():
    global pin_accumulate_dic
    pin_accumulate_dic = {}
