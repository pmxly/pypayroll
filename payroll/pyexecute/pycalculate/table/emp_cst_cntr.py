# -*- coding: utf-8 -*-


class EmpCostCenter:
    """
    Desc: 人员成本中心
    """

    def __init__(self, **kwargs):
        self.efft_date = kwargs.get('hhr_efft_date', None)
        self.cost_center_cd = '' if kwargs.get('hhr_cost_center_cd', '') is None else kwargs.get('hhr_cost_center_cd')
        self.ratio = 0 if kwargs.get('hhr_ratio', 0) is None else kwargs.get('hhr_ratio')
        self.inner_order = '' if kwargs.get('hhr_inner_order', '') is None else kwargs.get('hhr_inner_order')
        self.supp_desc = '' if kwargs.get('hhr_supp_desc', '') is None else kwargs.get('hhr_supp_desc')
