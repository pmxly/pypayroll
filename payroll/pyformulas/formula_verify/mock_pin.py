from sqlalchemy import text
from ....dbengine import DataBaseAlchemy


class Segment:
    def __init__(self):
        self.pin_id = ''
        self.segment_num = 0
        self.round_rule_id = ''
        self.amt_ = 1000
        self.currency_ = ''
        self.prcs_flag = ''
        self.bgn_dt = None
        self.end_dt = None
        self.std_amt_ = 1000
        self.retro_amt = 1000
        self.quantity_ = 8
        self.quantity_unit = ''
        self.ratio = 1
        self.init_amt = 1000
        self.init_currency = ''
        self.seg_rule_id = ''
        self.formula_id = ''
        self.acc_type = ''

    @property
    def amt(self):
        return self.amt_

    @amt.setter
    def amt(self, amt):
        if self.prcs_flag == '':
            self.amt_ = amt

    @property
    def currency(self):
        return self.currency_

    @currency.setter
    def currency(self, currency_cd):
        if self.prcs_flag == '':
            self.currency_ = currency_cd

    @property
    def std_amt(self):
        return self.std_amt_

    @std_amt.setter
    def std_amt(self, amt):
        if self.prcs_flag == '':
            self.std_amt_ = amt

    @property
    def quantity(self):
        return self.quantity_

    @quantity.setter
    def quantity(self, quantity):
        if self.prcs_flag == '':
            self.quantity_ = quantity


class Pin:
    def __init__(self, pin_id):
        self.pin_id_ = pin_id
        # 类型A-收入，B-扣减，C-合计，D-其他
        self.pin_type = ''
        # 处理标识 C-覆盖，U-不处理
        self.prcs_flag = ''
        self.pin_class =''
        self.base_flag = ''
        self.reg_flag = ''
        self.onetime_flag = ''
        self.seg_flag = ''
        self.seg_rule_id = ''
        self.round_rule_id = ''
        self.segment = {}
        self.amt_ = 1000
        self.quantity_ = 8
        self.std_amt_ = 1000
        self.retro_amt_ = 1000

        for i in range(1, 31):
            seg = Segment()
            seg.pin_id = self.pin_id
            seg.segment_num = i
            seg.seg_rule_id = self.seg_rule_id
            seg.round_rule_id = self.round_rule_id
            seg.bgn_dt = None
            seg.end_dt = None
            self.segment[i] = seg

        seg = Segment()
        seg.pin_id = self.pin_id
        seg.segment_num = '*'
        seg.seg_rule_id = self.seg_rule_id
        seg.round_rule_id = self.round_rule_id
        seg.bgn_dt = None
        seg.end_dt = None
        self.segment['*'] = seg

    @property
    def pin_id(self):
        return self.pin_id_

    @pin_id.setter
    def pin_id(self, pin_id_val):
        for seg in self.segment.values():
            seg.pin_id = pin_id_val

    @property
    def quantity(self):
        for seg in self.segment.values():
            self.quantity_ += seg.quantity
        return self.quantity_

    @property
    def amt(self):
        for seg in self.segment.values():
            self.amt_ += seg.amt
        return self.amt_

    @property
    def std_amt(self):
        for seg in self.segment.values():
            self.std_amt_ += seg.std_amt
        return self.std_amt_

    @property
    def retro_amt(self):
        for seg in self.segment.values():
            self.retro_amt_ += seg.retro_amt
        return self.retro_amt_


def get_pin_dic(tenant_id):
    pin_dic = {}
    db = DataBaseAlchemy()
    sql = text("select distinct a.hhr_pin_cd from boogoo_payroll.hhr_py_pin a where tenant_id = :b1 and "
               "a.hhr_status = 'Y' order by a.hhr_efft_date desc ")
    result = db.conn.execute(sql, b1=tenant_id).fetchall()
    if result is not None:
        for result_line in result:
            pin_cd = result_line['hhr_pin_cd']
            pin_dic[pin_cd] = Pin(pin_cd)
    db.conn.close()
    return pin_dic
