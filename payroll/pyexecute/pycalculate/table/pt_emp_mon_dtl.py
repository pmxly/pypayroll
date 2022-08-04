# -*- coding: utf-8 -*-


class PtEmpMonDtl:
    """
    Desc: 员工考勤月报表
    Author: David
    Date: 2020/12/31
    """

    __slots__ = [
        'tenant_id',
        'year',
        'month',
        'pt_code',
        'belong_year',
        'belong_month',
        'empid',
        'amount'
    ]

    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.year = kwargs.get('hhr_year', 1900)
        self.month = kwargs.get('hhr_month', 0)
        self.pt_code = kwargs.get('hhr_pt_code', '')
        self.belong_year = kwargs.get('hhr_belong_year', 1900)
        self.belong_month = kwargs.get('hhr_belong_month', 0)
        self.empid = kwargs.get('hhr_empid', '')
        self.amount = kwargs.get('hhr_amount', 0)
