# coding:utf-8
"""
此文件包含运行过程中用到的对象，运行过程和运行过程里的行
create by 王岭 2018/7/19
"""
from ..pysysutils import global_variables as gv
from sqlalchemy import select
from sqlalchemy.sql import text
# from ..pyformulas import create_formula as formula
from datetime import datetime


class ProcessChild:
    """
    运行过程中包含的元素
    create by wangling 2018/7/18
    """

    def __init__(self, tenant_id, run_process_id, seq_num, chi_type, chi_id, start_date, end_date, country):

        # 租户ID
        self.tenant_id = tenant_id
        # 运行过程唯一id
        self.run_process_id = ''
        # 运行过程元素序号，关键字，升序
        self.seq_num = ''
        # FC-函数，FM-公式，WC-薪资项目累计,WT-薪资项目
        self.chi_type = ''
        # 元素ID
        self.chi_id = ''
        # 运行类型对应的国家
        self.country = 'ALL'
        # 元素开始日期
        self.start_date = None
        # 元素结束日期
        self.end_date = None
        # 元素实体
        self.element_entity = None

        if country == 'ALL':
            self.country = gv.get_var_value('VR_F_COUNTRY')

        self.run_process_id = run_process_id
        self.seq_num = seq_num
        self.chi_type = chi_type
        self.chi_id = chi_id
        self.country = country
        self.start_date = start_date
        if end_date is None:
            end_date = datetime(9999, 12, 31).date()
        self.end_date = end_date

    def element_exec(self):
        # from ..pyformulas import create_formula as formula
        if self.element_entity is not None:
            if self.chi_type == "FM":
                # formula.add_lists(self.element_entity)
                # formula.validate_pins(self.element_entity)
                self.element_entity.formula_exec()
            elif self.chi_type == "FC":
                self.element_entity.func_exec()
            elif self.chi_type == "WT":
                if gv.pin_in_dic(self.chi_id):
                    self.element_entity.formula_exec()
            elif self.chi_type == "WC":
                self.element_entity.accumulate_exec()


class RunProcess:
    """
    运行过程
    create by wangling 2018/7/18
    """

    def __init__(self, tenant_id, run_process_id):
        # 租户ID
        self.tenant_id = tenant_id
        # 运行过程唯一id
        self.id = ''
        # 变量所属国家ALL:所有国家 CHN:中国
        self.country = 'ALL'
        self.description = ''

        self.id = run_process_id
        # 行数据
        self.data = []
        db = gv.get_db()
        stmt = text("select hhr_country, hhr_description from boogoo_payroll.hhr_py_runprocess where tenant_id = :b1 "
                    "and hhr_runprocess_id = :b2 ")
        result = db.conn.execute(stmt, b1=tenant_id, b2=run_process_id).fetchone()
        if result is not None:
            self.country = result["hhr_country"]
            self.description = result["hhr_description"]
        else:
            pass

        t = db.get_table("hhr_py_runprocess_child", schema_name='boogoo_payroll')
        result = db.conn.execute(
            select([t.c.hhr_seqnum, t.c.hhr_element_type, t.c.hhr_element_id, t.c.hhr_start_dt, t.c.hhr_end_dt],
                   (t.c.tenant_id == tenant_id) &
                   (t.c.hhr_runprocess_id == run_process_id)).order_by(t.c.hhr_seqnum)).fetchall()
        if result is not None:
            for result_line in result:
                child = ProcessChild(tenant_id, run_process_id, result_line['hhr_seqnum'], result_line['hhr_element_type'],
                                     result_line['hhr_element_id'], result_line['hhr_start_dt'],
                                     result_line['hhr_end_dt'], self.country)
                self.data.append(child)
