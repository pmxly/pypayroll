# -*- coding: utf-8 -*-


from ..pysysutils import global_variables as gv
from sqlalchemy import text, select


class Bracket:
    """
    Desc: 分级
    Author: David
    Date: 2018/08/06
    """

    __slots__ = [
        'tenant_id',          # 租户ID
        'bracket_cd',         # 分级编码
        'country',            # 所属国家ALL:所有国家 CHN:中国
        'eff_date',           # 生效日期
        'status',             # 状态Y-有效，N-无效
        'bracket_name',       # 分级名称
        'search_method',      # 查找方法: L-使用下一个较低值，H-使用下一个较高值，E-使用精确值
        'search_keys',
        'return_values',
        'data',
    ]

    def __init__(self, tenant_id, bracket_cd, eff_date):
        self.tenant_id = tenant_id
        self.bracket_cd = bracket_cd

        self.search_keys = []          # 搜索关键字
        self.return_values = []        # 返回值
        self.data = []                 # 数据记录

        db = gv.get_db()
        s = text("select a.hhr_efft_date, a.hhr_country, a.hhr_status, a.hhr_description, a.hhr_search_method, "
                 "a.hhr_elem_type_key1, a.hhr_elem_cd_key1, a.hhr_elem_type_key2, a.hhr_elem_cd_key2, "
                 "a.hhr_elem_type_key3, a.hhr_elem_cd_key3, a.hhr_elem_type_key4, a.hhr_elem_cd_key4, "      
                 "a.hhr_elem_type_key5, a.hhr_elem_cd_key5, " 
                 "a.hhr_elem_type_val1, a.hhr_elem_cd_val1, a.hhr_elem_type_val2, a.hhr_elem_cd_val2, "
                 "a.hhr_elem_type_val3, a.hhr_elem_cd_val3, a.hhr_elem_type_val4, a.hhr_elem_cd_val4, "
                 "a.hhr_elem_type_val5, a.hhr_elem_cd_val5 "
                 "from boogoo_payroll.hhr_py_bracket a where a.tenant_id = :b1 and a.hhr_bracket_cd = :b2 "
                 "and :b3 between a.hhr_efft_date and a.hhr_efft_end_date and a.hhr_status = 'Y' ")
        result = db.conn.execute(s, b1=tenant_id, b2=bracket_cd, b3=eff_date).fetchone()

        if result is not None:
            self.eff_date = result["hhr_efft_date"]
            self.country = result["hhr_country"]
            self.status = result["hhr_status"]
            self.bracket_name = result["hhr_description"]
            self.search_method = result["hhr_search_method"]

            # 关键字
            etype_k1 = result["hhr_elem_type_key1"]
            ecd_k1 = result["hhr_elem_cd_key1"]
            if ecd_k1 is not None:
                o = gv.get_var_obj(ecd_k1)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_key1_dt'
                    elif t == 'float':
                        f = 'hhr_data_key1_dec'
                    elif t == 'string':
                        f = 'hhr_data_key1'
                    else:
                        f = ''
                    self.search_keys.append((etype_k1, ecd_k1, o, f))

            etype_k2 = result["hhr_elem_type_key2"]
            ecd_k2 = result["hhr_elem_cd_key2"]
            if ecd_k2 is not None:
                o = gv.get_var_obj(ecd_k2)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_key2_dt'
                    elif t == 'float':
                        f = 'hhr_data_key2_dec'
                    elif t == 'string':
                        f = 'hhr_data_key2'
                    else:
                        f = ''
                    self.search_keys.append((etype_k2, ecd_k2, o, f))

            etype_k3 = result["hhr_elem_type_key3"]
            ecd_k3 = result["hhr_elem_cd_key3"]
            if ecd_k3 is not None:
                o = gv.get_var_obj(ecd_k3)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_key3_dt'
                    elif t == 'float':
                        f = 'hhr_data_key3_dec'
                    elif t == 'string':
                        f = 'hhr_data_key3'
                    else:
                        f = ''
                    self.search_keys.append((etype_k3, ecd_k3, o, f))

            etype_k4 = result["hhr_elem_type_key4"]
            ecd_k4 = result["hhr_elem_cd_key4"]
            if ecd_k4 is not None:
                o = gv.get_var_obj(ecd_k4)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_key4_dt'
                    elif t == 'float':
                        f = 'hhr_data_key4_dec'
                    elif t == 'string':
                        f = 'hhr_data_key4'
                    else:
                        f = ''
                    self.search_keys.append((etype_k4, ecd_k4, o, f))

            etype_k5 = result["hhr_elem_type_key5"]
            ecd_k5 = result["hhr_elem_cd_key5"]
            if ecd_k5 is not None:
                o = gv.get_var_obj(ecd_k5)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_key5_dt'
                    elif t == 'float':
                        f = 'hhr_data_key5_dec'
                    elif t == 'string':
                        f = 'hhr_data_key5'
                    else:
                        f = ''
                    self.search_keys.append((etype_k5, ecd_k5, o, f))

            # 返回值
            etype_v1 = result["hhr_elem_type_val1"]
            ecd_v1 = result["hhr_elem_cd_val1"]
            if ecd_v1 is not None:
                o = gv.get_var_obj(ecd_v1)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_val1_dt'
                    elif t == 'float':
                        f = 'hhr_data_val1_dec'
                    elif t == 'string':
                        f = 'hhr_data_val1'
                    else:
                        f = ''
                    self.return_values.append((etype_v1, ecd_v1, o, f))

            etype_v2 = result["hhr_elem_type_val2"]
            ecd_v2 = result["hhr_elem_cd_val2"]
            if ecd_v2 is not None:
                o = gv.get_var_obj(ecd_v2)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_val2_dt'
                    elif t == 'float':
                        f = 'hhr_data_val2_dec'
                    elif t == 'string':
                        f = 'hhr_data_val2'
                    else:
                        f = ''
                    self.return_values.append((etype_v2, ecd_v2, o, f))

            etype_v3 = result["hhr_elem_type_val3"]
            ecd_v3 = result["hhr_elem_cd_val3"]
            if ecd_v3 is not None:
                o = gv.get_var_obj(ecd_v3)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_val3_dt'
                    elif t == 'float':
                        f = 'hhr_data_val3_dec'
                    elif t == 'string':
                        f = 'hhr_data_val3'
                    else:
                        f = ''
                    self.return_values.append((etype_v3, ecd_v3, o, f))

            etype_v4 = result["hhr_elem_type_val4"]
            ecd_v4 = result["hhr_elem_cd_val4"]
            if ecd_v4 is not None:
                o = gv.get_var_obj(ecd_v4)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_val4_dt'
                    elif t == 'float':
                        f = 'hhr_data_val4_dec'
                    elif t == 'string':
                        f = 'hhr_data_val4'
                    else:
                        f = ''
                    self.return_values.append((etype_v4, ecd_v4, o, f))

            etype_v5 = result["hhr_elem_type_val5"]
            ecd_v5 = result["hhr_elem_cd_val5"]
            if ecd_v5 is not None:
                o = gv.get_var_obj(ecd_v5)
                if o is not None:
                    t = o.data_type
                    if t == 'datetime':
                        f = 'hhr_data_val5_dt'
                    elif t == 'float':
                        f = 'hhr_data_val5_dec'
                    elif t == 'string':
                        f = 'hhr_data_val5'
                    else:
                        f = ''
                    self.return_values.append((etype_v5, ecd_v5, o, f))

            # 数据记录
            bd = db.get_table('hhr_py_bracket_data', schema_name='boogoo_payroll')
            s1 = select([bd], (bd.c.tenant_id == tenant_id)
                        & (bd.c.hhr_bracket_cd == bracket_cd)
                        & (bd.c.hhr_efft_date == self.eff_date))
            result1 = db.conn.execute(s1).fetchall()
            for row in result1:
                self.data.append(dict(row))
