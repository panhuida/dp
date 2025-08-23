-- /******************************************************************************
--    Author : panhuida
--    Name   : 日志表
--    Purpose  : dw.etl_log
--    Revisions or Comments
--    VER        DATE        AUTHOR           DESCRIPTION
--  ---------  ----------  ---------------  ------------------------------------
--  
-- ******************************************************************************/
DO
$$
DECLARE
    v_mapping_name     VARCHAR;
    v_etl_date         VARCHAR;
    v_begin_date       VARCHAR;
    v_end_date         VARCHAR;
    v_status           INT;
    v_error_info       VARCHAR;
    v_parameter_values VARCHAR;
    v_run_flg          INT;
    v_last_runtime     DATE;
    v_timeout_minutes  INT := 30;
    v_max_run_minutes  INT := 120;
    v_parameter_begin_date VARCHAR;
    v_parameter_end_date VARCHAR;
BEGIN
    v_mapping_name := %(mapping_name)s;
    v_etl_date := %(end_date)s;
    v_begin_date := %(log_start_time)s;
    v_end_date := %(log_end_time)s;
    v_status := %(status)s;
    v_error_info := %(error_info)s;
    v_parameter_values := %(parameter_values)s;
    v_parameter_begin_date := %(begin_date)s;
    v_parameter_end_date := %(end_date)s;    

    -- 此处的等待、超时预警等功能，放在任务编排中以及单独的监控中实现
    -- --当传入的status值为-1时，默认为需判断过程是否已在执行中
    -- IF v_status = -1 THEN
    --     LOOP
    --         --获取过程最后一次执行时的执行状态和执行时间
    --         SELECT COALESCE(MAX(a.status), 0), COALESCE(MAX(a.begin_date), CURRENT_DATE)
    --             INTO v_run_flg, v_last_runtime
    --             FROM (SELECT t.status,
    --                         t.begin_date,
    --                         ROW_NUMBER()
    --                         OVER (PARTITION BY t.mapping_name ORDER BY t.begin_date DESC, t.status DESC) rn
    --                     FROM dw.etl_log t
    --                     WHERE t.mapping_name = v_mapping_name
    --                     AND t.begin_date > CURRENT_DATE - 7) a
    --             WHERE rn = 1;
    --         --若执行状态为成功(0)或者失败(1)，
    --         --或者最后一次任务持续执行时间超过限制(last_run_minutes)
    --         --则退出循环
    --         EXIT WHEN v_run_flg >= 0 OR NOW() - v_last_runtime > (v_max_run_minutes || ' minutes')::INTERVAL;

    --         --若任务检测循环持续执行时间超过限制（timeout_minutes），则直接结束执行。
    --         IF NOW() > v_begin_date::TIMESTAMP + (v_timeout_minutes || ' minutes')::INTERVAL THEN
    --             v_status := -9999;
    --             RAISE EXCEPTION 'ERROR';
    --         END IF;

    --         --若有任务正在执行中，则60秒之后重新检测。
    --         PERFORM PG_SLEEP(60);

    --     END LOOP;
    -- END IF;


    -- --插入日志表
    -- INSERT INTO dw.etl_log (id, mapping_name, etl_date, begin_date, end_date, status, error_info, parameter_values)
    -- SELECT RPAD(REGEXP_REPLACE(v_etl_date, '[-:, ]', '', 'g'), 14, '0') id,
    --        v_mapping_name mapping_name,
    --        v_etl_date::TIMESTAMP etl_date,
    --        v_begin_date::TIMESTAMP begin_date,
    --        CASE WHEN v_end_date = '' THEN NULL ELSE v_end_date::TIMESTAMP END end_date,
    --        v_status status,
    --        v_error_info error_info,
    --        v_parameter_values parameter_values
    --     ON CONFLICT (id, mapping_name)
    --         DO UPDATE
    --         SET etl_date         = excluded.etl_date,
    --             begin_date       = excluded.begin_date,
    --             end_date         = excluded.end_date,
    --             status           = excluded.status,
    --             error_info       = excluded.error_info,
    --             parameter_values = excluded.parameter_values;

-- 插入日志表
-- 日志表的主键，按数据表的ETL执行周期和ETL名称
insert into public.etl_log 
(
 id
,etl_name
,etl_type
,etl_params
,etl_params_start_date
,etl_params_end_date
,start_datetime
,end_datetime
,status
,error_info
,create_datetime
,update_datetime
,delete_flag
)
select 
     concat(v_parameter_begin_date,'_', v_mapping_name) id  -- 唯一主键
    ,v_mapping_name etl_name  -- etl名称
    ,null etl_type  -- etl类型
    ,v_parameter_values etl_params  -- etl入参
    ,v_parameter_begin_date::timestamp etl_params_start_date  -- etl入参开始时间
    ,v_parameter_end_date::timestamp etl_params_end_date  -- etl入参结束时间
    ,v_begin_date::timestamp start_datetime  -- etl执行时间
    ,case when v_end_date = '' then null else v_end_date::timestamp end end_datetime  -- etl结束时间
    ,v_status status  -- etl状态
    ,v_error_info error_info  -- etl异常信息
    ,localtimestamp create_datetime  -- 创建时间
    ,localtimestamp update_datetime  -- 更新时间
    ,'0' delete_flag  -- 删除标识
on conflict (id)
do update set
     etl_params = excluded.etl_params
    ,etl_params_start_date = excluded.etl_params_start_date
    ,etl_params_end_date = excluded.etl_params_end_date
    ,start_datetime = excluded.start_datetime
    ,end_datetime = excluded.end_datetime
    ,status = excluded.status
    ,error_info = excluded.error_info
    ,update_datetime = excluded.update_datetime
    ,delete_flag = excluded.delete_flag
;

END;
$$
;   