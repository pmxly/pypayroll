# coding:utf-8
from ..pysysutils import global_variables as gv
from sqlalchemy import text
from decimal import Decimal


class VariableObject:
    """
    变量的基类
    create by wangling 2018/6/27
    """

    def __init__(self, tenant_id, v_id, value_dic):
        """
        变量构造函数
        :param tenant_id:租户ID
        :param v_id:变量ID
        :param value_dic:包含变量属性值的字典，如为空构造函数根据系统ID和变量ID在数据库中查询装载
        """

        self.tenant_id = tenant_id     # 租户ID
        self.id = v_id           # 变量唯一id
        self.desc = ''           # 变量描述，用于系统中的描述
        self.descENG = ''        # 变量英文描述，用于系统中的描述
        self.data_type = ''      # 变量类型datetime/float/string
        self.var_type = ''       # 变量类型S：系统标量,C:客户化变量
        self.country = 'ALL'     # 变量所属国家ALL:所有国家 CHN:中国
        self.cover_enable = 'N'  # 是否可以覆盖
        self.has_covered = 'N'   # 是否已经被覆盖

        # 选中变量后默认带出来的变量文本
        # self.edit_text = "VAR['" + id + "'].value"
        # 变量使用中文说明
        # self.instructions_ = '可以在公式中直接使用：' + id + '代表变量'
        # 变量使用英文说明
        # self.instructionsENG_ = 'In formula,：' + id + ' can be used directly'

        self.value_ = None       # 变量值

        if len(value_dic) != 0:
            self.set_value(value_dic)
        else:
            self.select_variable()

    def set_value(self, result):
        """
        根据结果集自动初始化变量属性
        :param result:包含结果的集合可能是字典，可能是sqlalchemy.engine.result.RowProxy类型
        :return:NONE
        """
        self.tenant_id = result['tenant_id']
        self.id = result['hhr_variable_id']
        self.desc = result['hhr_description']
        self.data_type = result['hhr_data_type'].lower()
        self.country = result['hhr_country']
        self.cover_enable = result['hhr_cover_enable']
        if self.data_type == 'datetime':
            self.value_ = None
        elif self.data_type == 'string':
            self.value_ = ''
        elif self.data_type == 'float':
            self.value_ = 0
        self.var_type = result['hhr_var_type']
        # self.edit_text = result['HHR_EDIT_TEXT']

    def select_variable(self):
        """
        根据系统ID和变量ID在数据库中查出其他属性
        :return:
        """
        db = gv.get_db()
        sql_var = text("select tenant_id, hhr_variable_id, hhr_description, hhr_data_type, hhr_country, "
                       "hhr_cover_enable, hhr_edit_text, hhr_var_type from boogoo_payroll.hhr_py_variable "
                       "where tenant_id = :b1 and hhr_variable_id = :b2 ")
        result = db.conn.execute(sql_var, b1=self.tenant_id, b2=self.id).fetchone()
        if result is not None:
            self.set_value(result)
        else:
            pass

    # 获取变量值
    @property
    def value(self):
        return self.value_

    # 设置变量值
    @value.setter
    def value(self, value):
        if self.has_covered == 'N':
            if self.data_type == 'float':
                if value is None:
                    value = 0
                self.value_ = Decimal(str(value)).quantize(Decimal('0.0000'))
            else:
                self.value_ = value


class SysVariableObject(VariableObject):
    """
        系统变量的基类
        create by wangling 2018/6/28
        """

    # 变量类型S：系统标量,C:客户化变量
    var_type = 'S'
