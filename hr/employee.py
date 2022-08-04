class EmployeeRecord:
    __slots__ = [
        'tenant_id',
        'emp_id',
        'emp_rcd',
        'posn_cd',
        'base_on_posn_sw',
        'dir_rpt_posn',
        'dot_rpt_posn',
        'direct_empid',
        'direct_emp_rcd',
        'indirect_empid',
        'indirect_emp_rcd'
    ]

    def __init__(self, **kwargs):
        self.tenant_id = kwargs.get('tenant_id', 0)
        self.emp_id = kwargs.get('emp_id', '')
        self.emp_rcd = kwargs.get('emp_rcd', 0)
        self.posn_cd = kwargs.get('posn_cd', '')
        self.base_on_posn_sw = kwargs.get('base_on_posn_sw', '')
        self.dir_rpt_posn = kwargs.get('dir_rpt_posn', '')
        self.dot_rpt_posn = kwargs.get('dot_rpt_posn', '')
        self.direct_empid = kwargs.get('direct_empid', '')
        self.direct_emp_rcd = kwargs.get('direct_emp_rcd', 0)
        self.indirect_empid = kwargs.get('indirect_empid', '')
        self.indirect_emp_rcd = kwargs.get('indirect_emp_rcd', 0)
