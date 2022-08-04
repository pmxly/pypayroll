#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ..pysysutils import global_variables as gv
from sqlalchemy import text


class SegRule:
    """
    Desc: 分段规则
    Author: David
    Date: 2018/08/02
    """

    __slots__ = [
        'tenant_id',      # 租户ID
        'seg_rule_cd',    # 分段规则编码
        'country',        # 所属国家ALL:所有国家 CHN:中国
        'eff_date',       # 生效日期
        'seg_rule_name',  # 分段规则名称
        'numerator',      # 分子
        'denominator',    # 分母
        'fixed_days',     # 固定天数
        'priority',       # 优先级
    ]

    def __init__(self, tenant_id, seg_rule_cd, eff_date):
        self.tenant_id = tenant_id
        self.seg_rule_cd = seg_rule_cd

        db = gv.get_db()
        s = text("select a.hhr_seg_rule_name, a.hhr_efft_date, a.hhr_country, a.hhr_numerator, a.hhr_denominator,"
                 "a.hhr_fixed_days, a.hhr_priority "
                 "from boogoo_payroll.hhr_py_seg_rule a where a.tenant_id = :b1 and a.hhr_seg_rule_cd = :b2 and "
                 ":b3 between a.hhr_efft_date and a.hhr_efft_end_date "
                 # "  a.hhr_efft_date = (select max(a1.hhr_efft_date) from boogoo_payroll.hhr_py_seg_rule a1 "
                 # "  where a1.tenant_id = a.tenant_id and a1.hhr_seg_rule_cd = a.hhr_seg_rule_cd "
                 # "  and a1.hhr_efft_date <= :b3) "
                 "and a.hhr_status = 'Y' ")
        result = db.conn.execute(s, b1=tenant_id, b2=seg_rule_cd, b3=eff_date).fetchone()
        if result is not None:
            self.country = result["hhr_country"]
            self.eff_date = result["hhr_efft_date"]
            self.seg_rule_name = result["hhr_seg_rule_name"]
            self.numerator = result["hhr_numerator"]
            self.denominator = result["hhr_denominator"]
            self.fixed_days = result["hhr_fixed_days"]
            self.priority = result["hhr_priority"]
