# coding:utf-8
"""
薪资项目/元素
"""
from ..pysysutils import global_variables as gv
from sqlalchemy import text
from ..pyformulas.create_formula import create
from ..pysysutils.func_lib_02 import round_rule
from ..pysysutils.py_calc_log import log


class Segment:
    """
    薪资项目分段信息
    """
    def __init__(self):
        # 薪资项目ID
        self.pin_id = ''
        # 分段号
        self.segment_num = 0
        self.round_rule_id = ''
        # 实际金额
        self.amt_ = 0
        # 货币
        self.currency_ = ''
        # 处理标识 C-覆盖，U-不处理，S-明细类收入扣减汇总
        self.prcs_flag = ''
        # 开始日期
        self.bgn_dt = None
        # 结束日期
        self.end_dt = None
        # 标准金额
        self.std_amt_ = 0
        # 追溯金额
        self.retro_amt = 0
        # 数量
        self.quantity_ = 0
        # 数量单位
        self.quantity_unit = ''
        # 比率
        self.ratio = 0
        # 初始金额
        self.init_amt = 0
        # 初始货币
        self.init_currency = ''
        # 分段规则
        self.seg_rule_id = ''
        # 公式ID
        self.formula_id = ''
        # 累计类型 （P-期间）
        self.acc_type = ''

    @property
    def amt(self):
        return self.amt_

    @amt.setter
    def amt(self, amt):
        if self.prcs_flag == '':
            self.amt_ = round_rule(self.round_rule_id, amt)

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
            self.std_amt_ = round_rule(self.round_rule_id, amt)

    @property
    def quantity(self):
        return self.quantity_

    @quantity.setter
    def quantity(self, quantity):
        if self.prcs_flag == '':
            self.quantity_ = round_rule(self.round_rule_id, quantity)


class Pin:
    def __init__(self, tenant_id, pin_id, eff_date):
        """
        构造函数，初始化薪资项目属性
        :param tenant_id: 租户ID
        :param pin_id: 薪资元素ID
        :param eff_date: 生效日期
        """
        self.tenant_id = tenant_id
        self.pin_id_ = pin_id
        db = gv.get_db()
        sql = text(
            "select a.hhr_status,a.hhr_efft_date,a.hhr_country,a.hhr_description,a.hhr_short_description,a.hhr_pin_type, "
            "a.hhr_pin_class,a.hhr_pin_basic,a.hhr_pin_regular,a.hhr_pin_onetime,a.hhr_round_rule,a.hhr_segment_flag,a.hhr_seg_rule_cd,"
            "a.hhr_formula_id from boogoo_payroll.hhr_py_pin a where a.tenant_id =:b1 and a.hhr_pin_cd =:b2 and a.hhr_efft_date<= :b3 order by hhr_efft_date desc")
        result = db.conn.execute(sql, b1=tenant_id, b2=pin_id, b3=eff_date).fetchone()
        self.eff_date = result['hhr_efft_date']
        self.status = result['hhr_status']
        self.country = result['hhr_country']
        self.description = result['hhr_description']
        self.short_name = result['hhr_short_description']
        # 类型A-收入，B-扣减，C-合计，D-其他
        self.pin_type = result['hhr_pin_type']
        # 处理标识 C-覆盖，U-不处理
        self.prcs_flag = ''
        self.pin_class = result['hhr_pin_class']
        self.base_flag = result['hhr_pin_basic']
        self.reg_flag = result['hhr_pin_regular']
        self.onetime_flag = result['hhr_pin_onetime']
        self.seg_flag = result['hhr_segment_flag']
        self.seg_rule_id = result['hhr_seg_rule_cd']
        self.round_rule_id = result['hhr_round_rule']
        self.formula_id = result['hhr_formula_id']
        if self.formula_id:
            self.formula_entity = create(tenant_id, result['HHR_FORMULA_ID'])
        if self.round_rule_id is None:
            self.round_rule_id = gv.get_run_var_value('CUR_CAL_OBJ').py_group_entity.round_rule

        if self.seg_flag == 'Y':
            # 如果勾选了分段标识，但未填分段规则，就默认薪资组分段规则，如果薪资组分段规则为空，表示薪资项目不分段
            if self.seg_rule_id == '' or self.seg_rule_id is None:
                self.seg_rule_id = gv.get_run_var_value('CUR_CAL_OBJ').py_group_entity.seg_rule_cd
                if self.seg_rule_id == '' or self.seg_rule_id is None:
                    self.seg_flag = 'N'
        self.segment = {}
        self.amt_ = 0
        self.quantity_ = 0
        self.std_amt_ = 0
        self.retro_amt_ = 0

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.pin_id,
                'desc': self.description,
                'type': 'WT',
                'wt_obj': self
            }
        else:
            self.trace_dic = {}

    def create_segment(self, seg_info):
        """
        薪资项目分段处理
        :param seg_info:
        :return:
        """

        catalog = gv.get_run_var_value('PY_CATALOG')
        f_bgn_dt = catalog.f_prd_bgn_dt
        f_end_dt = catalog.f_prd_end_dt

        if self.seg_flag == 'Y':
            seg_items_dic = seg_info.seg_items_dic['*']
            for i in range(1, len(seg_items_dic) + 1):
                seg = Segment()
                seg.pin_id = self.pin_id
                seg.segment_num = i
                seg.seg_rule_id = self.seg_rule_id
                seg.round_rule_id = self.round_rule_id
                seg_item = seg_items_dic[i]
                seg.bgn_dt = seg_item.bgn_dt
                seg.end_dt = seg_item.end_dt
                seg.formula_id = self.formula_id
                self.segment[i] = seg
        else:
            seg = Segment()
            seg.pin_id = self.pin_id
            seg.segment_num = '*'
            seg.seg_rule_id = self.seg_rule_id
            seg.round_rule_id = self.round_rule_id
            seg.formula_id = self.formula_id
            seg.bgn_dt = f_bgn_dt
            seg.end_dt = f_end_dt
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
        temp_quantity = 0
        for seg in self.segment.values():
            temp_quantity += seg.quantity
        self.quantity_ = temp_quantity
        return self.quantity_

    # @quantity.setter
    # def quantity(self, value):
    #     if self.seg_flag == 'Y':
    #         for seg in self.segment.values():
    #             seg.quantity = value
    #     else:
    #         self.segment['*'].amt = value

    @property
    def amt(self):
        temp_amt = 0
        for seg in self.segment.values():
            temp_amt += seg.amt
        self.amt_ = temp_amt
        return self.amt_

    # @amt.setter
    # def amt(self, value):
    #     if self.seg_flag == 'Y':
    #         for seg in self.segment.values():
    #             seg.amt = value
    #     else:
    #         self.segment['*'].amt = value

    @property
    def std_amt(self):
        temp_std_amt = 0
        for seg in self.segment.values():
            temp_std_amt += seg.std_amt
        self.std_amt_ = temp_std_amt
        return self.std_amt_

    @property
    def retro_amt(self):
        temp_retro_amt = 0
        for seg in self.segment.values():
            temp_retro_amt += seg.retro_amt
        self.retro_amt_ = temp_retro_amt
        return self.retro_amt_

    @log()
    def formula_exec(self):
        # 判断是否已处理该薪资项目，不存在时继续处理；存在且处理列为空，继续处理；存在且处理列为C或U，停止处理
        if self.prcs_flag == '' and self.formula_id.strip() != '':
            self.formula_entity.formula_exec()
