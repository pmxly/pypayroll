# coding:utf-8
"""
薪资项目累计
Created by David on 2018/8/22
"""

from sqlalchemy import text
from ..pysysutils import global_variables as gv
from .pinaccum_util import get_accumulate_value
from ..pysysutils.func_lib_02 import get_acc_year_seq
from ..pyexecute.pycalculate.table.pin_seg import PinSeg
from ..pyexecute.pycalculate.table.accm_pin import PinAccTable
from ..pysysutils.py_calc_log import log
from copy import deepcopy
from decimal import Decimal


class Segment:
    """
    薪资项目累加器分段信息
    create by wangling 2018/8/22
    """

    def __init__(self):
        # 分段号
        self.segment_num = 0
        self.seg_rule_id = ''
        # 标准金额
        self.std_amt_ = 0
        # 实际金额
        self.amt_ = 0
        # 数量
        self.quantity = 0
        # 数量单位
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


class PinAccChild:
    """
    薪资项目累加器项目明细
    create by wangling 2018/8/23
    """

    def __init__(self):
        # 租户ID
        self.tenant_id = ''
        # 国家
        self.country = ''
        # 薪资项目累计编码
        self.acc_code = ''
        # 元素代码
        self.pin_code = ''
        # 开始日期
        self.start_dt = None
        # 结束日期
        self.end_dt = None
        # 加减运算符，+：加法运算，-：减法运算
        self.operator = ''
        # 条件变量
        self.variable_id = ''
        # 表示是否累计该薪资项目
        self.is_valid = False


class PinAccumulate:
    """
     薪资项目累加器项实体
    create by wangling 2018/8/23
    """

    def __init__(self, tenant_id, acc_code, country, f_end_date, pg_currency):
        # 租户ID
        self.tenant_id = ''
        # 薪资项目累计编码
        self.acc_code = ''
        # 国家
        self.country = ''
        # 描述
        self.description = ''
        # 累计期间，P-期间;M-月度;Q-季度;U-非限制;X-半年度;Y-年度
        self.acc_type = ''
        # 消除分段，Y：消除，N：不消除
        self.disable_seg = ''
        # 累计金额
        self.amt_ = 0
        self.std_amt_ = 0
        self.quantity_ = 0

        self.tenant_id = tenant_id
        self.acc_code = acc_code
        self.country = country
        # 薪资项目列表
        self.pins = []
        # 累加器分段信息
        self.segment = {}
        self.acc_year_seq = None
        self.description = ''
        self.acc_type = ''
        self.disable_seg = ''
        self.pg_currency = pg_currency
        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.acc_code,
                'desc': self.description,
                'type': 'WC',
                'wc_obj': self
            }
        else:
            self.trace_dic = {}

        sql = text('select a.hhr_description,a.hhr_accm_type,a.hhr_remove_seg from boogoo_payroll.hhr_py_pin_acc a where a.tenant_id=:b1 and a.hhr_acc_cd=:b2 and a.hhr_country=:b3 ')
        db = gv.get_db()
        result = db.conn.execute(sql, b1=tenant_id, b2=acc_code, b3=country).fetchone()
        if result is not None:
            self.description = result['hhr_description']
            self.acc_type = result['hhr_accm_type']
            self.disable_seg = result['hhr_remove_seg']

        sql = text(
            'select a.hhr_pin_cd,a.hhr_start_dt,a.hhr_end_dt,a.hhr_add_subtract,a.hhr_variable_id from boogoo_payroll.hhr_py_pin_acc_line '
            'a where a.tenant_id=:b1 and a.hhr_acc_cd=:b2 and a.hhr_start_dt <= :b3 and (a.hhr_end_dt >= :b3 or a.hhr_end_dt is null) ')
        result = db.conn.execute(sql, b1=tenant_id, b2=acc_code, b3=f_end_date).fetchall()
        for result_line in result:
            child = PinAccChild()
            child.tenant_id = tenant_id
            child.acc_code = acc_code
            child.country = country
            child.pin_code = result_line['hhr_pin_cd']
            child.start_dt = result_line['hhr_start_dt']
            child.end_dt = result_line['hhr_end_dt']
            child.operator = result_line['hhr_add_subtract']
            child.variable_id = result_line['hhr_variable_id']
            self.pins.append(child)
            # if gv.pin_in_dic(child.pin_code):
            #     if not child.variable_id:
            #         self.pins.append(child)
            #     elif gv.get_var_value(child.variable_id) == 1:
            #         self.pins.append(child)

    @property
    def amt(self):
        temp_amt = 0
        for seg in self.segment.values():
            temp_amt += seg.amt
        self.amt_ = temp_amt
        return self.amt_

    @property
    def std_amt(self):
        temp_amt = 0
        for seg in self.segment.values():
            temp_amt += seg.std_amt
        self.std_amt_ = temp_amt
        return self.std_amt_

    @property
    def quantity(self):
        temp_quantity = 0
        for seg in self.segment.values():
            temp_quantity += seg.quantity
        self.quantity_ = temp_quantity
        return self.quantity_

    def get_wc_pin_amt(self, pin_cd, seg_index, operator):
        """
        create by David 2018/12/12
        获取薪资项目金额
        :param pin_cd:当前薪资项目ID
        :param seg_index:分段号
        :param operator:操作符号
        :return:
        """

        amt = std_amt = quantity = 0
        this_pin = gv.get_pin(pin_cd)
        if this_pin.seg_flag == 'Y':
            segment = this_pin.segment
            # 注意：考勤处理可能会导致某个薪资项目的分段数不等于期间分段数
            if seg_index in segment:
                seg = segment[seg_index]
                amt, std_amt, quantity = seg.amt, seg.std_amt, seg.quantity
            else:
                amt, std_amt, quantity = 0, 0, 0
        else:
            seg_items = gv.get_run_var_value('SEG_INFO_OBJ')
            max_seg_num = len(seg_items.seg_items_dic['*'])
            # 如果累计项目消除分段，则不分段的薪资项目只累计一次(在最后一个分段上累计)
            if self.disable_seg == 'Y':
                if seg_index == max_seg_num:
                    seg = this_pin.segment['*']
                    amt, std_amt, quantity = seg.amt, seg.std_amt, seg.quantity
                else:
                    amt = std_amt = quantity = 0
            # 如果累计项目不消除分段，则可以重复累计不分段的薪资项目
            elif self.disable_seg == 'N':
                seg = this_pin.segment['*']
                amt, std_amt, quantity = seg.amt, seg.std_amt, seg.quantity
        if operator == '+':
            amt = + amt
            std_amt = + std_amt
            quantity = + quantity
        else:
            amt = - amt
            std_amt = - std_amt
            quantity = - quantity

        return amt, std_amt, quantity

    def accumulate_segment(self, seg_index):
        """
        按分段号执行累加
        :param seg_index:分段号
        :return:
        """

        seg_accumulate = Segment()
        for wc_child in self.pins:
            if wc_child.is_valid:
                if gv.pin_in_dic(wc_child.pin_code):
                    if wc_child.operator != '':
                        amt, std_amt, quantity = self.get_wc_pin_amt(wc_child.pin_code, seg_index, wc_child.operator)
                        seg_accumulate.amt += amt
                        seg_accumulate.std_amt += std_amt
                        seg_accumulate.quantity += quantity
        seg_accumulate.segment_num = seg_index
        self.segment[seg_index] = seg_accumulate

    def merge_segment(self):
        """
        消除合并分段
        :return:
        """
        seg_merge = Segment()
        for seg in self.segment.values():
            seg_merge.std_amt = seg.std_amt
            seg_merge.amt += seg.amt
            seg_merge.quantity += seg.quantity

        self.segment = dict()
        self.segment['*'] = seg_merge

    def accumulate_by_type(self):
        """
        按累计类型累加
        :return:
        """

        # 非期间累计的（累计类型P、T除外的累计类型），从累计日历表中取对应累计类型的累计年度、累计序号
        if self.acc_type != 'P' and self.acc_type != 'T':
            self.acc_year_seq = get_acc_year_seq(self.acc_type)
            acc_tuple = get_accumulate_value(acc_obj=self, acc_year_seq=self.acc_year_seq)
            self.segment['*'].amt += acc_tuple[0]
            self.segment['*'].quantity += acc_tuple[1]
        elif self.acc_type == 'T':
            # 若累计类型为T（税务年度），从上一序号对应的薪资项目累计表中取值时，累计类型、累计年度、累计序号使用的是T、VR_TAX_YEAR、VR_TAX_NO
            vr_tax_year = gv.get_var_value('VR_TAX_YEAR')
            vr_tax_no = gv.get_var_value('VR_TAX_NO')
            acc_year_seq = (vr_tax_year, vr_tax_no)
            self.acc_year_seq = acc_year_seq
            """判断变量VR_TAX_PRE的值：值为1则不获取前期累计的值 Modified by David on 20190529"""
            vr_tax_pre = gv.get_var_value('VR_TAX_PRE')
            if vr_tax_pre != 1:
                acc_tuple = get_accumulate_value(acc_obj=self, acc_year_seq=acc_year_seq)
                self.segment['*'].amt += acc_tuple[0]
                self.segment['*'].quantity += acc_tuple[1]

    @log()
    def accumulate_exec(self):
        """
        累加器执行累加方法，执行后自动按分段累加累加器中的薪资项目并将结果存入self.segments中
        :return:
        """

        # trans = gv.get_db().conn.begin() 2021-02-02

        catalog = gv.get_run_var_value('PY_CATALOG')
        f_bgn_dt = catalog.f_prd_bgn_dt
        f_end_dt = catalog.f_prd_end_dt

        pg_currency = self.pg_currency
        seg_items = deepcopy(gv.get_run_var_value('SEG_INFO_OBJ'))

        for seg_index in range(1, len(seg_items.seg_items_dic['*']) + 1):
            self.accumulate_segment(seg_index)

        if self.disable_seg == 'Y':
            self.merge_segment()
            self.accumulate_by_type()

        # 插入薪资项目表/薪资项目累计表
        if self.disable_seg == 'Y':
            if self.acc_type == 'P':
                seg = self.segment['*']
                seg.segment_num = '*'
                seg.pin_id = self.acc_code
                seg.currency = pg_currency
                seg.prcs_flag = ''
                seg.bgn_dt = f_bgn_dt
                seg.end_dt = f_end_dt
                seg.retro_amt = 0.0
                seg.ratio = 0.0
                seg.init_amt = 0.0
                seg.init_currency = ''
                seg.seg_rule_id = ''
                seg.round_rule_id = ''
                seg.formula_id = ''
                seg.acc_type = 'P'
                if self.amt != 0 or self.std_amt != 0 or self.quantity != 0:
                    check_and_delete_exists_pin_seg(catalog.tenant_id, catalog.emp_id, catalog.emp_rcd, catalog.seq_num, seg.pin_id, seg.segment_num)
                    v_dic = {'tenant_id': catalog.tenant_id, 'emp_id': catalog.emp_id, 'emp_rcd': catalog.emp_rcd, 'seq_num': catalog.seq_num, 'seg': seg}
                    PinSeg(**v_dic).insert()
            else:
                seg = self.segment['*']
                if self.amt != 0 or self.std_amt != 0 or self.quantity != 0:
                    check_and_delete_exists_pin_acc(catalog.tenant_id, catalog.emp_id, catalog.emp_rcd, catalog.seq_num, self.acc_code, self.acc_type)
                    v_dic = {'tenant_id': catalog.tenant_id, 'emp_id': catalog.emp_id,
                             'emp_rcd': catalog.emp_rcd, 'seq_num': catalog.seq_num,
                             'acc_cd': self.acc_code, 'acc_type': self.acc_type, 'add_year': self.acc_year_seq[0],
                             'add_num': self.acc_year_seq[1], 'amt': seg.amt, 'currency': pg_currency,
                             'quantity': seg.quantity, 'quantity_unit': seg.quantity_unit}
                    PinAccTable(**v_dic).insert()
        else:
            # 不消除分段的薪资项目累计都是P类型
            items_dic = seg_items.seg_items_dic['*']
            for seg_num, seg_item in items_dic.items():
                seg = self.segment[seg_num]
                seg.pin_id = self.acc_code
                seg.currency = pg_currency
                seg.prcs_flag = ''
                seg.bgn_dt = seg_item.bgn_dt
                seg.end_dt = seg_item.end_dt
                seg.retro_amt = 0.0
                seg.ratio = 0.0
                seg.init_amt = 0.0
                seg.init_currency = ''
                seg.seg_rule_id = ''
                seg.round_rule_id = ''
                seg.formula_id = ''
                seg.acc_type = self.acc_type
                if self.amt != 0 or self.std_amt != 0 or self.quantity != 0:
                    check_and_delete_exists_pin_seg(catalog.tenant_id, catalog.emp_id, catalog.emp_rcd, catalog.seq_num, seg.pin_id, seg.segment_num)
                    v_dic = {'tenant_id': catalog.tenant_id, 'emp_id': catalog.emp_id,
                             'emp_rcd': catalog.emp_rcd, 'seq_num': catalog.seq_num, 'seg': seg}
                    PinSeg(**v_dic).insert()
        gv.set_pin_acc(self)

        # trans.commit() 2021-02-02


def check_and_delete_exists_pin_seg(tenant_id, emp_id, emp_rcd, seq_num, pin_cd, segment_num):
    """
    判断是否已经存在相同的累计项目（比如重复累计的情况）
    """
    if segment_num == "*":
        segment_num = 999
    db = gv.get_db()
    # t = text("select hhr_pin_cd from boogoo_payroll.hhr_py_rslt_pin where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
    #          "and hhr_seq_num = :b4 and hhr_pin_cd = :b5 and hhr_segment_num = :b6")
    # row = db.conn.execute(t, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=pin_cd, b6=segment_num).fetchone()
    # if row is not None:
    #     d = text("delete from boogoo_payroll.hhr_py_rslt_pin where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
    #              "and hhr_seq_num = :b4 and hhr_pin_cd = :b5 and hhr_segment_num = :b6")
    #     db.conn.execute(d, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=pin_cd, b6=segment_num)

    d = text("delete from boogoo_payroll.hhr_py_rslt_pin where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
                 "and hhr_seq_num = :b4 and hhr_pin_cd = :b5 and hhr_segment_num = :b6")
    db.conn.execute(d, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=pin_cd, b6=segment_num)


def check_and_delete_exists_pin_acc(tenant_id, emp_id, emp_rcd, seq_num, acc_cd, accm_type):
    db = gv.get_db()
    t = text("select hhr_acc_cd from boogoo_payroll.hhr_py_rslt_accm where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
             "and hhr_seq_num = :b4 and hhr_acc_cd = :b5 and hhr_accm_type = :b6")
    row = db.conn.execute(t, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=acc_cd, b6=accm_type).fetchone()
    if row is not None:
        d = text("delete from boogoo_payroll.hhr_py_rslt_accm where tenant_id = :b1 and hhr_empid = :b2 and hhr_emp_rcd = :b3 "
                 "and hhr_seq_num = :b4 and hhr_acc_cd = :b5 and hhr_accm_type = :b6")
        db.conn.execute(d, b1=tenant_id, b2=emp_id, b3=emp_rcd, b4=seq_num, b5=acc_cd, b6=accm_type)


def get_accumulate_in_dev(tenant_id, acc_code, country, pg_currency, f_end_date):
    """
    获取薪资项目累计，如果DEV变量中没有则创建
    :param tenant_id:租户ID
    :param acc_code:薪资项目累计ID
    :param country:国家地区
    :param pg_currency: 货币
    :param f_end_date: 历经期结束日期
    :return:薪资项目累计
    """

    accumulate_dic = gv.get_run_var_value('ALL_ACCUMULATE_DIC')
    dict_key = (acc_code, country, f_end_date)
    if dict_key not in accumulate_dic:
        accumulate_dic[dict_key] = PinAccumulate(tenant_id, acc_code, country, f_end_date, pg_currency)
        gv.set_run_var_value('ALL_ACCUMULATE_DIC', accumulate_dic)
    return accumulate_dic[dict_key]


def get_new_accumulate(tenant_id, acc_code, country, pg_currency, f_end_date):
    """
    获取薪资项目累计，如果DEV变量中没有则创建
    :param tenant_id:租户ID
    :param acc_code:薪资项目累计ID
    :param country:国家地区
    :param pg_currency: 货币
    :param f_end_date: 历经期结束日期
    :return:薪资项目累计
    """

    accumulate_dic = gv.get_run_var_value('ALL_ACCUMULATE_DIC')
    dict_key = (acc_code, country, f_end_date)

    acc = PinAccumulate(tenant_id, acc_code, country, f_end_date, pg_currency)
    accumulate_dic[dict_key] = acc
    gv.set_run_var_value('ALL_ACCUMULATE_DIC', accumulate_dic)

    return acc
