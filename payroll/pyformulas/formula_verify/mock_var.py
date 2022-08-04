from sqlalchemy import text
from ....dbengine import DataBaseAlchemy
import datetime


class VariableObject:

    def __init__(self, v_id):
        self.id = v_id           # 变量唯一id
        self.desc = ''           # 变量描述，用于系统中的描述
        self.descENG = ''        # 变量英文描述，用于系统中的描述
        self.data_type = ''      # 变量类型datetime/float/string
        self.var_type = ''       # 变量类型S：系统标量,C:客户化变量
        self.country = 'ALL'     # 变量所属国家ALL:所有国家 CHN:中国
        self.cover_enable = 'N'  # 是否可以覆盖
        self.has_covered = 'N'   # 是否已经被覆盖
        self.value_ = None       # 变量值

    @property
    def value(self):
        return self.value_

    @value.setter
    def value(self, value):
        self.value_ = value


def get_variable_dic(tenant_id):
    var_dic = {}
    db = DataBaseAlchemy()
    sql = text("select distinct a.hhr_variable_id, a.hhr_data_type from boogoo_payroll.hhr_py_variable a "
               "where a.tenant_id = :b1 and a.hhr_status = 'Y' ")
    result = db.conn.execute(sql, b1=tenant_id).fetchall()
    for result_line in result:
        var_id = result_line['hhr_variable_id']
        data_type = result_line['hhr_data_type'].lower()
        var_obj = VariableObject(var_id)
        if data_type == 'datetime':
            var_obj.value_ = datetime.datetime(1900, 1, 1)
        elif data_type == 'string':
            var_obj.value_ = 'abc'
        elif data_type == 'float':
            var_obj.value_ = 1
        var_dic[var_id] = var_obj
    db.conn.close()
    return var_dic
