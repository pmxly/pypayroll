# coding:utf-8

from ..pyfunctions.function_object import FunctionObject
from ..pysysutils import global_variables as gv
from ..pysysutils.py_calc_log import log
from sqlalchemy import text
from datetime import datetime, date


class PyFunction(FunctionObject):
    """
    Desc: 获取岗位信息函数
    Author: David
    Date: 2020/12/18
    """

    __slots__ = ['id', 'country', 'desc', 'descENG',
                 'func_type', 'instructions', 'instructionsENG']

    def __init__(self):
        super(PyFunction, self).__init__()

        self.id = 'FC_GET_POSN_JOB'
        self.country = 'CHN'
        self.desc = '获取岗位对应的职位函数'
        self.descENG = '获取岗位对应的职位函数'
        self.func_type = 'A'
        self.instructions = "获取岗位对应的职位函数。输入参数：岗位编码、日期（未指定时默认历经期结束日期）；输出参数：职位、职族、职类、职种"
        self.instructionsENG = self.instructions

        self.log_flag = gv.get_run_var_value('LOG_FLAG')
        if self.log_flag == 'Y':
            self.trace_dic = {
                'id': self.id,
                'desc': self.desc,
                'type': 'FC',
                'fc_obj': self,
                'WC': [],
                'WT': [],
                'VR': [],
                'PA': ['posn_cd', 'to_date']
            }
        else:
            self.trace_dic = {}

    @log()
    def func_exec(self, posn_cd, to_date=None):
        if posn_cd is None or posn_cd == '':
            raise Exception("函数FC_GET_POSN_JOB的参数错误")

        if to_date is None:
            prd_end_dt = gv.get_var_value('VR_F_PERIOD_END')
            to_date = prd_end_dt

        if isinstance(to_date, str):
            try:
                to_date = datetime.strptime(to_date, "%Y-%m-%d")
            except (TypeError, ValueError):
                try:
                    to_date = datetime.strptime(to_date, "%Y/%m/%d")
                except (TypeError, ValueError):
                    raise Exception("函数FC_GET_POSN_JOB的参数错误")
        elif not isinstance(to_date, date):
            raise Exception("函数FC_GET_POSN_JOB的参数错误")

        catalog = gv.get_run_var_value('PY_CATALOG')
        tenant_id = catalog.tenant_id

        dict_key = str(tenant_id) + "_" + posn_cd + "_" + str(to_date)
        posn_job_dic = gv.get_run_var_value('POSN_JOB_DIC')
        if dict_key not in posn_job_dic:
            db = gv.get_db()
            catalog = gv.get_run_var_value('PY_CATALOG')
            t = text("select j.hhr_group_code,j.hhr_sequence_code,j.hhr_org_job_attr01 "
                     "from boogoo_corehr.hhr_org_posn p LEFT JOIN boogoo_corehr.hhr_org_job j "
                     "ON j.tenant_id = p.tenant_id AND j.hhr_job_code = p.hhr_job_code "
                     "AND p.hhr_efft_date BETWEEN j.hhr_efft_date and j.hhr_efft_end_date "
                     "WHERE p.tenant_id = :b1 AND p.hhr_posn_code = :b2 AND :b3 BETWEEN p.hhr_efft_date AND p.hhr_efft_end_date")
            r = db.conn.execute(t, b1=catalog.tenant_id, b2=posn_cd, b3=to_date).fetchone()
            if r is not None:
                group_code = r['hhr_group_code']
                seq_code = r['hhr_sequence_code']
                job_attr01 = r['hhr_org_job_attr01']
                posn_job_dic[dict_key] = (group_code, seq_code, job_attr01)
                return group_code, seq_code, job_attr01
            else:
                return '', '', ''
        else:
            return posn_job_dic[dict_key]
