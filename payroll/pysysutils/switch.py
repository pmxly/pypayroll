# coding:utf-8

from ..pysysutils import global_variables as gv
from sqlalchemy import text


# 获取控制开关值
def get_switch_values(tenant_id, sw_cds_lst):
    sw_val_dic = {}
    sw_cds_str = ""
    for sw_cd in sw_cds_lst:
        sw_val_dic[sw_cd] = ""
        sw_cds_str = sw_cds_str + "'" + sw_cd + "'" + ","
    sw_cds_str = sw_cds_str.strip(",")
    if sw_cds_str:
        db = gv.get_db()
        sql = text("select sw.hhr_cfg_sw_code, sw.hhr_cfg_sw_val from boogoo_foundation.hhr_switch_cfg sw "
                   "where sw.tenant_id = :b1 and sw.hhr_cfg_sw_code in (" + sw_cds_str +  ") ")
        result = db.conn.execute(sql, b1=tenant_id).fetchall()
        for row in result:
            cfg_sw_cd = row['hhr_cfg_sw_code']
            cfg_sw_val = row['hhr_cfg_sw_val']
            sw_val_dic[cfg_sw_cd] = cfg_sw_val
    return sw_val_dic
