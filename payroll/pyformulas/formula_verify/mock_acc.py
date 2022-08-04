from sqlalchemy import text
from decimal import Decimal
from ....dbengine import DataBaseAlchemy


class Segment:
    def __init__(self):
        self.segment_num = 0
        self.seg_rule_id = ''
        self.std_amt_ = 1000
        self.amt_ = 1000
        self.quantity = 8
        self.quantity_unit = ''

    @property
    def amt(self):
        return self.amt_

    @amt.setter
    def amt(self, amt):
        self.amt_ = Decimal(str(amt)).quantize(Decimal('0.0000'))

    @property
    def std_amt(self):
        return self.std_amt_

    @std_amt.setter
    def std_amt(self, amt):
        self.std_amt_ = Decimal(str(amt)).quantize(Decimal('0.0000'))


class PinAccumulate:

    def __init__(self, acc_code):
        self.tenant_id = ''
        self.acc_code = acc_code
        self.country = ''
        self.description = ''
        self.acc_type = ''
        self.disable_seg = ''
        self.amt_ = 1000
        self.std_amt_ = 1000
        self.quantity_ = 8
        self.acc_code = acc_code
        self.country = ''
        self.pins = []
        self.segment = {}
        self.acc_year_seq = None
        self.description = ''
        self.acc_type = ''
        self.disable_seg = ''
        self.pg_currency = ''

        for seg_index in range(1, 31):
            self.accumulate_segment(seg_index)

        seg = Segment()
        seg.std_amt = 1000
        seg.amt = 1000
        seg.quantity = 8
        seg.segment_num = '*'
        self.segment['*'] = seg

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
    def quantity(self):
        for seg in self.segment.values():
            self.quantity_ += seg.quantity
        return self.quantity_

    def accumulate_segment(self, seg_index):
        seg_accumulate = Segment()
        seg_accumulate.amt = 1000
        seg_accumulate.std_amt = 1000
        seg_accumulate.quantity = 8
        seg_accumulate.segment_num = seg_index
        self.segment[seg_index] = seg_accumulate


def get_pin_acc_dic(tenant_id):
    acc_dic = {}
    db = DataBaseAlchemy()
    sql = text('select distinct a.hhr_acc_cd from boogoo_payroll.hhr_py_pin_acc a where tenant_id = :b1')
    result = db.conn.execute(sql, b1=tenant_id).fetchall()
    if result is not None:
        for result_line in result:
            acc_cd = result_line['hhr_acc_cd']
            acc_dic[acc_cd] = PinAccumulate(acc_cd)
    db.conn.close()
    return acc_dic
