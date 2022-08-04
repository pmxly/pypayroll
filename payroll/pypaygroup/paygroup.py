#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from ..pysysutils import global_variables as gv
from sqlalchemy import text
from ..pysegrule.seg_rule import SegRule


class PayGroup:
    """
    Desc: 薪资组
    Author: David
    Date: 2018/08/03
    """

    def __init__(self, tenant_id, pay_group_id, eff_date):
        self.tenant_id = ''            # 租户ID
        self.pay_group_id = ''         # 薪资组编码
        self.country = 'CHN'           # 所属国家ALL:所有国家 CHN:中国
        self.eff_date = None           # 生效日期
        self.status = ''               # 状态Y-有效，N-无效
        self.pay_group_name = ''       # 薪资组名称

        self.period_cd = ''            # 期间ID
        self.retro_flag = ''           # 追溯标志,Y-追溯，N-不追溯
        self.seg_rule_cd = ''          # 分段规则
        self.max_retro_prd_num = 0     # 最大回溯期间数
        self.round_rule = ''           # 取整规则
        self.work_plan = ''            # 工作计划
        self.currency = ''             # 货币
        self.pay_bank = ''             # 支付银行

        self.abs_eval_rule = ''        # 考勤评估规则
        self.abs_prd_diff_sw = ''      # 考勤期间差异
        self.abs_period = ''           # 考勤期间
        self.prd_seq_diff_sw = ''      # 期间序号差异
        self.diff_num = 0              # 差异数
        self.abs_cycle_std_sw = ''     # 采用考勤周期薪资标准

        self.seg_rule_entity = None    # 分段规则实体

        self.tenant_id = tenant_id
        self.pay_group_id = pay_group_id

        db = gv.get_db()
        s = text("select p.hhr_country, p.hhr_efft_date, p.hhr_status, p.hhr_description, p.hhr_period_code, p.hhr_retro_flag, "
                 "p.hhr_seg_rule_cd, p.hhr_max_retro_prd_num, p.hhr_round_rule, p.hhr_work_plan, p.hhr_currency, p.hhr_payment_bank, "
                 "p.hhr_abs_eval_rule, p.hhr_abs_prd_diff_sw, p.hhr_abs_period, p.hhr_prd_seq_diff_sw, p.hhr_diff_num, p.hhr_abs_cycl_std_sw "
                 "from boogoo_payroll.hhr_py_paygroup p where p.tenant_id = :b1 and p.hhr_pygroup_id = :b2 "
                 "and :b3 between p.hhr_efft_date and p.hhr_efft_end_date "
                 "and p.hhr_status = 'Y' ")
        result = db.conn.execute(s, b1=tenant_id, b2=pay_group_id, b3=eff_date).fetchone()
        if result is not None:
            self.country = result["hhr_country"]
            self.eff_date = result["hhr_efft_date"]
            self.status = result["hhr_status"]
            self.pay_group_name = result["hhr_description"]
            self.period_cd = result["hhr_period_code"]
            self.retro_flag = result["hhr_retro_flag"]
            self.seg_rule_cd = result["hhr_seg_rule_cd"]
            self.max_retro_prd_num = result["hhr_max_retro_prd_num"]
            self.round_rule = result["hhr_round_rule"]
            self.work_plan = result["hhr_work_plan"]
            self.currency = result["hhr_currency"]
            self.pay_bank = result["hhr_payment_bank"]
            self.abs_eval_rule = result["hhr_abs_eval_rule"]
            self.abs_prd_diff_sw = result["hhr_abs_prd_diff_sw"]
            self.abs_period = result["hhr_abs_period"]
            self.prd_seq_diff_sw = result["hhr_prd_seq_diff_sw"]
            self.diff_num = result["hhr_diff_num"]
            self.abs_cycle_std_sw = result["hhr_abs_cycl_std_sw"]
            self.seg_rule_entity = SegRule(tenant_id, result["hhr_seg_rule_cd"], result["hhr_efft_date"])
