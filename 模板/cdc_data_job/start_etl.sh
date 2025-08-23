#!/bin/bash
#Nacos配置文件地址
# usage="-s http://172.16.4.168:8848 -n prod -u nacos -p Config@Nacos2090 -d data-service.yml -g DEFAULT_GROUP"
#ETL计算周期
# begin_date=$(date -d "-1 hour" +"%Y-%m-%d %H:00:00")
# end_date=$(date +"%Y-%m-%d %H:00:00")
begin_date=$(date -d "-1 day" +"%Y-%m-%d")
end_date=$(date +"%Y-%m-%d")
#ETL程序主目录
cd /data/dp
#执行ETL程序
# python3 main.py $usage --sql_list_file etl/hour_sql_list --sql_params begin_date="$begin_date",end_date="$end_date"
#python3 main.py $usage --file etl/dwd_event_record.sql --sql_params begin_date="$begin_date",end_date="$end_date"
python3 main.py --sql_list_file etl/hour_sql_list --sql_params begin_date="$begin_date",end_date="$end_date"    
python3 main.py --sql_list_file etl/etl_end --sql_params begin_date="$begin_date",end_date="$end_date"   