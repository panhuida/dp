-- /******************************************************************************
--    Author : hdpan
--    Name   : 检验项目维表
--    Purpose  : dw.dim_dict
--    Revisions or Comments
--    VER        DATE        AUTHOR           DESCRIPTION
--  ---------  ----------  ---------------  ------------------------------------
--    1.0      2022-07-20    hdpan           1、用于检验数据查询页面检验结果定性，但此字典表也用来维护各种字典数据
--    
-- ******************************************************************************/

DO
$$
DECLARE
        v_begin_date   VARCHAR(20);
        v_end_date     VARCHAR(20);
        v_mapping_name VARCHAR(50);
BEGIN
        v_begin_date := %(begin_date)s;
        v_end_date := %(end_date)s;
        v_mapping_name := 'dim_dict';
        IF v_begin_date IS NULL OR v_begin_date = '' THEN
SELECT MAX(t.etl_date) - INTERVAL '5 minutes' --获取五分钟的重叠增量，防止数据同步延迟
INTO v_begin_date
FROM dw.etl_log t
WHERE t.mapping_name = v_mapping_name --传入的过程名
  AND t.status = 1; --状态为执行完毕
END IF;
  
insert into dw.dim_dict
(
 dict_category
,dict_type
,dict_code
,dict_name
,dict_value_code
,dict_value_name
,dict_source
,memo
,create_datetime
,update_datetime
)
select
 null dict_category  -- 字典种类
,'dwd_ch_inspection_report' dict_type  -- 字典类型
,'abnormal' dict_code  -- 字典编码
,'检验结果定性' dict_name  -- 字典名称
,a.abnormal dict_value_code  -- 字典值编码
,a.abnormal dict_value_name  -- 字典值名称
,'ETL' dict_source  -- 字典来源
,'接口' memo  -- 备注
,localtimestamp create_datetime  -- 创建时间
,localtimestamp update_datetime  -- 更新时间
from 
(
select a.abnormal
 from dw.dwd_ch_inspection_report a
where a.etl_update_datetime >= v_begin_date::timestamp
  and a.etl_update_datetime <  v_end_date::timestamp
group by a.abnormal
) a
where length(a.abnormal)>=1  -- 去掉空值、空字符串
on conflict (dict_type, dict_code, dict_value_code, dict_value_name)
do update set
dict_category = EXCLUDED.dict_category,
dict_type = EXCLUDED.dict_type,
dict_code = EXCLUDED.dict_code,
dict_name = EXCLUDED.dict_name,
dict_value_code = EXCLUDED.dict_value_code,
dict_value_name = EXCLUDED.dict_value_name,
dict_source = EXCLUDED.dict_source,
memo = EXCLUDED.memo,
update_datetime = EXCLUDED.update_datetime
;


-- 20220922，V1.0.8.1，健康校园字典同步
insert into dw.dim_dict
(
 dict_category
,dict_type
,dict_code
,dict_name
,dict_value_code
,dict_value_name
,dict_source
,memo
,create_datetime
,update_datetime
)
select 
 'mc' dict_category  -- 字典种类
,'SCHOOL_CHECK' dict_type  -- 字典类型
,'SYMPTOM_TYPE' dict_code  -- 字典编码
,'晨午晚检症状' dict_name  -- 字典名称
,a.dict_code dict_value_code  -- 字典值编码
,a.dict_name dict_value_name  -- 字典值名称
,'ETL' dict_source  -- 字典来源
,'tb_cdcmc_dict' memo  -- 备注
,localtimestamp create_datetime  -- 创建时间
,localtimestamp update_datetime  -- 更新时间
from ods.ods_mc_dict a 
where a.etl_update_datetime >= v_begin_date::timestamp
  and a.etl_update_datetime <  v_end_date::timestamp
  and a.module_code = 'SCHOOL_CHECK'
  and a.dict_type = 'SYMPTOM_TYPE'  -- 晨午晚检症状  
on conflict (dict_type, dict_code, dict_value_code, dict_value_name)
do nothing
;

END;
$$
