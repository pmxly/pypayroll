# coding:utf-8
"""
此文件包含运行类型中用到的对象，运行过程和运行过程里的行
create by 王岭 2018/7/19
"""
from ..pysysutils import global_variables as gv
from sqlalchemy import select
from .run_process import RunProcess as RunProcess


class RunTypeChild:
    """
    运行类型中包含的元素
    create by wangling 2018/7/18
    """

    def __init__(self, tenant_id, process_type_id, run_process_id, seq_num):
        # 租户ID
        self.tenant_id = ''
        # 运行类型唯一id
        self.process_type_id = ''
        # 运行类型元素序号，关键字，升序
        self.seq_num = ''
        # 运行过程ID
        self.run_process_id = ''
        # 状态，是否启用'A':启用 'I'：不启用
        self.active = 'A'
        # 运行过程对象
        self.run_process = None

        self.tenant_id = tenant_id
        self.process_type_id = process_type_id
        self.run_process_id = run_process_id
        self.seq_num = seq_num
        self.run_process = RunProcess(tenant_id, run_process_id)


class RunType:
    """
    运行类型
    create by wangling 2018/7/18
    """

    def __init__(self, tenant_id, process_type_id):
        # 租户ID
        self.tenant_id = ''
        # 运行类型唯一id
        self.process_type_id = ''
        # 所属国家ALL:所有国家 CHN:中国  (无ALL)
        self.country = 'CHN'
        # 周期 ‘C’：周期，‘O’：非周期
        self.cycle = ''

        self.tenant_id = tenant_id
        self.process_type_id = process_type_id
        # 行数据
        self.data = []
        db = gv.get_db()
        t = db.get_table("hhr_py_runtype", schema_name='boogoo_payroll')
        result = db.conn.execute(select([t.c.hhr_country, t.c.hhr_cycle],
                                        (t.c.tenant_id == tenant_id) &
                                        (t.c.hhr_runtype_id == process_type_id))).fetchone()
        if result is not None:
            self.country = result["hhr_country"]
            self.cycle = result["hhr_cycle"]
            t1 = db.get_table("hhr_py_runtype_child", schema_name='boogoo_payroll')
            result = db.conn.execute(select([t1.c.hhr_runprocess_id, t1.c.hhr_seqnum],
                                            (t1.c.tenant_id == tenant_id) &
                                            (t1.c.hhr_runtype_id == process_type_id) &
                                            (t1.c.hhr_active == 'Y')).order_by(t1.c.hhr_seqnum)).fetchall()
            if result is not None:
                for result_line in result:
                    self.data.append(RunTypeChild(tenant_id, process_type_id, result_line['hhr_runprocess_id'], result_line['hhr_seqnum']))
