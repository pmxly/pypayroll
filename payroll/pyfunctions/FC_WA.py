# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from sqlalchemy import text
from ..pysysutils.py_calc_log import log, add_fc_log_item


class PyFunction(FunctionObject):
    """
    Desc: 可写数组函数
    Author: David
    Date: 2018/11/08
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()
        
        self.id = 'FC_WA'
        self.country = 'CHN'
        self.desc = '可写数组函数'
        self.descENG = '可写数组函数'
        self.func_type = 'B'
        self.instructions = '可写数组函数'
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
                'VR': ['VR_F_COUNTRY'],
                'PA': ['wa_id']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, wa_id):
        """
        :param wa_id: 可写数组ID
        :return:None
        """

        db = gv.get_db()
        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id
        emp_id = catalog.emp_id
        emp_rcd = catalog.emp_rcd
        seq_num = catalog.seq_num
        py_gp_country = gv.get_var_value('VR_F_COUNTRY')

        val_dic = {'tenant_id': tenant_id, 'hhr_empid': emp_id, 'hhr_emp_rcd': emp_rcd, 'hhr_seq_num': seq_num}

        table = None
        sql = text("select a.hhr_recname, b.hhr_fieldname, b.hhr_wa_elem_type, b.hhr_wa_elem_cd from boogoo_payroll.hhr_py_wa_cfg a, "
                   "boogoo_payroll.hhr_py_wa_flds b where a.tenant_id = b.tenant_id and a.hhr_wa_cd = b.hhr_wa_cd "
                   "and a.hhr_country = b.hhr_country and a.tenant_id = :b1 and a.hhr_wa_cd = :b2 and a.hhr_country = :b3 and a.hhr_status = 'Y' ")
        rs = db.conn.execute(sql, b1=tenant_id, b2=wa_id, b3=py_gp_country).fetchall()
        for row in rs:
            if table is None:
                table = row['hhr_recname']
            field = row['hhr_fieldname']

            elem_type = row['hhr_wa_elem_type']
            elem_cd = row['hhr_wa_elem_cd']
            add_fc_log_item(self, elem_type, elem_cd)

            elem_val = 0
            if elem_type == 'WT':
                if gv.pin_in_dic(elem_cd):
                    elem_val = gv.get_pin(elem_cd).amt
                else:
                    continue
            elif elem_type == 'WC':
                if gv.pin_acc_in_dic(elem_cd):
                    elem_val = gv.get_pin_acc(elem_cd).amt
                else:
                    continue
            elif elem_type == 'VR':
                if gv.is_var_exists(elem_cd):
                    elem_val = gv.get_var_value(elem_cd)
                else:
                    continue

            val_dic[field] = elem_val

        if table is None:
            raise Exception("可写数组" + str(wa_id) + "不存在")

        write_array = db.get_table(table, schema_name='boogoo_payroll')
        ins = write_array.insert()
        val = [val_dic]
        db.conn.execute(ins, val)
