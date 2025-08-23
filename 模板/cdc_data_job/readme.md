### 运行所需的库
```python
nacos-sdk-python==0.1.6
PyYAML==6.0
psycopg2==2.9.3
PyMySQL==1.0.2
```


### xxl-job配置示例

```shell
#!/bin/bash
#Nacos配置文件地址
usage="-s http://172.16.4.168:8848 -n prod -u nacos -p Config@Nacos2090 -d data-service.yml -g DEFAULT_GROUP"
#ETL计算周期
#begin_date=$(date -d "-1 hour" +"%Y-%m-%d %H:00:00")
#end_date=$(date +"%Y-%m-%d %H:00:00")
begin_date="2023-01-14 00:00:00"
end_date="2023-01-14 18:00:00"
#ETL程序主目录
cd /data/dp
#执行ETL程序
#要用双引号，单引号会和字典参数冲突
#python3 main.py $usage --sql_list_file etl/hour_sql_list --sql_params begin_date="$begin_date",end_date="$end_date"
#python3 main.py $usage --file etl/dwd_event_record.sql --sql_params begin_date="$begin_date",end_date="$end_date"
python3 main.py $usage --sql_list_file etl/min_sql_list_report_card --sql_params begin_date="$begin_date",end_date="$end_date"

exit $?
```



### 监控
1、监控任务没有运行或是超时，使用单独的程序，从数据库的任务日志表读取
未运行（起始任务） 
任务超时（任务执行状态：处理中、成功、失败）



### 备注
1、日志记录、监控，都和ETL的执行周期（执行周期与时间周期一致）有关