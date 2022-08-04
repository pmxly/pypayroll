# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pybracket.bracket import Bracket
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import add_fc_log_item
from ..pysysutils.py_calc_log import log


def reset_return_vars(rv):
    for k in range(len(rv)):
        ret_tup = rv[k]
        ret_var_obj = ret_tup[2]
        t = ret_var_obj.data_type
        v = None
        if t == 'datetime':
            v = None
        elif t == 'float':
            v = 0
        elif t == 'string':
            v = ''
        ret_var_obj.value = v


class PyFunction(FunctionObject):
    """
    Desc: 分级函数
    Author: David
    Date: 2018/08/06
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_BR'
        self.country = 'CHN'
        self.desc = '分级函数'
        self.descENG = 'Bracket function'
        self.func_type = 'B'  # 无返回值
        self.instructions = '分级函数，接收分级编码作为参数，具体输入项与返回项由分级配置页面指定'
        self.instructionsENG = self.instructions

        log_flag = gv.get_run_var_value('LOG_FLAG')
        if log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': [],
                'VR': [],
                'PA': ['bracket_cd']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, bracket_cd):
        """目前规定多个关键字的分级只是用精确匹配"""

        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id

        # (历经期)结束日期
        prd_end_dt = gv.get_var_value('VR_F_PERIOD_END')

        # bracket_dic = gv.get_run_var_value('BRACKET_DIC')
        # if prd_end_dt not in bracket_dic:
        #     bracket_dic[prd_end_dt] = {}
        #     bracket = Bracket(tenant_id, bracket_cd, prd_end_dt)
        #     bracket_dic[prd_end_dt][bracket_cd] = bracket
        #     gv.set_run_var_value('BRACKET_DIC', bracket_dic)
        #     new_bracket = copy(bracket)
        # else:
        #     if bracket_cd not in bracket_dic[prd_end_dt]:
        #         bracket = Bracket(tenant_id, bracket_cd, prd_end_dt)
        #         bracket_dic[prd_end_dt][bracket_cd] = bracket
        #         gv.set_run_var_value('BRACKET_DIC', bracket_dic)
        #         new_bracket = copy(bracket)
        #     else:
        #         new_bracket = copy(bracket_dic[prd_end_dt][bracket_cd])

        new_bracket = Bracket(tenant_id, bracket_cd, prd_end_dt)
        sm = new_bracket.search_method
        sk = new_bracket.search_keys
        rv = new_bracket.return_values
        data = new_bracket.data

        prev = set()
        if (len(sk) != 0) and (len(rv) != 0) and (len(data) != 0):
            # ----------------------- 方式 E ----------------------- #
            has_match_row = False
            mat_row_dict = {}
            for i in range(len(sk)):
                key_tup = sk[i]
                var_obj = key_tup[2]
                # 如果元素类型不是数值，默认采用精确查找方式
                if (var_obj.data_type != 'float') or (sm == 'E') or (len(sk) > 1):
                    data_key_fld = key_tup[3]
                    mat_row_dict[data_key_fld] = set()
                    for j in range(len(data)):
                        row_dic = data[j]

                        if var_obj.value == row_dic.get(data_key_fld):
                            # 记录每个关键字匹配的行号
                            mat_row_dict[data_key_fld].add(j + 1)
                            has_match_row = True

                    # 只要有一个关键字不匹配就结束
                    if (i == 0) and (len(mat_row_dict[data_key_fld]) == 0):
                        reset_return_vars(rv)
                        return

            # 针对每一个关键字判断是否有公共的匹配行
            if has_match_row:
                c = 0
                for v in mat_row_dict.values():
                    c += 1
                    if c == 1:
                        prev = v
                    else:
                        prev = prev & v
                if len(prev) == 0:
                    reset_return_vars(rv)
                    return

                # 如果每一个关键字都完成了匹配过程，就返回数据
                if len(mat_row_dict) == len(sk):
                    pos = prev.pop()
                    for k in range(len(rv)):
                        ret_tup = rv[k]
                        ret_var_obj = ret_tup[2]
                        data_ret_fld = ret_tup[3]
                        ret_var_obj.value = data[pos - 1].get(data_ret_fld)

                    return
            else:
                reset_return_vars(rv)

            # ----------------------- 方式 L/H ----------------------- #
            has_match_row = False
            mat_row_dict = {}
            for i in range(len(sk)):
                key_tup = sk[i]
                var_obj = key_tup[2]
                # 如果元素类型是数值且查找方式不是精确匹配
                if (var_obj.data_type == 'float') and (sm != 'E'):
                    data_key_fld = key_tup[3]
                    mat_row_dict[data_key_fld] = set()
                    # 构造类似{1: 600, 2: 500}结构
                    kv_dict = {}
                    for j in range(len(data)):
                        row = data[j]
                        kv_dict[(j + 1)] = row.get(data_key_fld)
                    # 构造类似[(2, 500), (1, 600)]排序结构
                    kv_tup_lst = sorted(kv_dict.items(), key=lambda x: x[1], reverse=True)
                    for m in range(len(kv_tup_lst)):
                        kv_tup = kv_tup_lst[m]
                        # L-使用下一个较低值
                        if sm == 'L':
                            if kv_tup[1] <= var_obj.value:
                                mat_row_dict[data_key_fld].add(kv_tup[0])
                                has_match_row = True
                                break
                        # H-使用下一个较高值
                        elif sm == 'H':
                            if kv_tup[1] < var_obj.value:
                                mat_row_dict[data_key_fld].add(kv_tup[0])
                                has_match_row = True
                                break

            if has_match_row:
                # 针对每一个关键字判断是否有公共的匹配行
                c = 0
                prev = set()
                for v in mat_row_dict.values():
                    c += 1
                    if c == 1:
                        prev = v
                    else:
                        prev = prev & v
                if len(prev) == 0:
                    return

                prev_l = list(prev)
                prev_l.sort()

                pos = None
                for k in range(len(rv)):
                    ret_tup = rv[k]
                    if sm == 'L':
                        pos = prev_l[len(prev_l) - 1]
                    elif sm == 'H':
                        pos = prev_l[0]
                    ret_tup[2].value = data[pos - 1].get(ret_tup[3])
            else:
                for k in range(len(rv)):
                    ret_tup = rv[k]
                    ret_var_obj = ret_tup[2]
                    t = ret_var_obj.data_type
                    v = None
                    if t == 'datetime':
                        v = None
                    elif t == 'float':
                        v = 0
                    elif t == 'string':
                        v = ''
                    ret_var_obj.value = v

        add_fc_log_item(self, 'VR', '', sk)
        add_fc_log_item(self, 'VR', '', rv)

