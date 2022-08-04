# coding:utf-8
"""
适用范围
"""
from ..pysysutils import global_variables as gv
from sqlalchemy import select


class EligibilityPins:
    """
    适用范围实体
    """

    def __init__(self, tenant_id, el_id):
        # 租户ID
        self.tenant_id = tenant_id
        # 适用范围ID
        self.el_id = el_id
        # 适用范围描述
        self.description = ''
        # 国家/地区
        self.country = ''
        # 适用范围包含的薪资项目
        self.el_pin_dic = {}
        db = gv.get_db()
        t = db.get_table('hhr_py_app_scope_head', schema_name='boogoo_payroll')
        result = db.conn.execute(
            select([t.c.hhr_app_scope_name, t.c.hhr_country]).where(
                t.c.tenant_id == tenant_id).where(t.c.hhr_app_scope_code == el_id)).fetchone()
        if result is not None:
            self.description = result['hhr_app_scope_name']
            self.country = result['hhr_country']
        else:
            pass

    def init_el_pin_dic(self):
        db = gv.get_db()
        t = db.get_table('hhr_py_app_scope_line', schema_name='boogoo_payroll')
        result = db.conn.execute(
            select([t.c.hhr_pin_cd, t.c.hhr_bgn_date, t.c.hhr_end_date]).where(
                t.c.tenant_id == self.tenant_id).where(t.c.hhr_app_scope_code == self.el_id)).fetchall()
        if result is not None:
            for result_line in result:
                self.el_pin_dic[result_line['hhr_pin_cd']] = EligibilityPinsChild(self.tenant_id, self.el_id,
                                                                                  result_line['hhr_pin_cd'],
                                                                                  result_line['hhr_bgn_date'],
                                                                                  result_line['hhr_end_date'])


class EligibilityPinsChild:
    """
    适用范围包含的薪资项目
    """

    def __init__(self, tenant_id, el_id, pin_code, start_dt, end_dt):
        self.tenant_id = tenant_id
        self.el_id = el_id
        self.pin_code = pin_code
        self.start_dt = start_dt
        self.end_dt = end_dt
