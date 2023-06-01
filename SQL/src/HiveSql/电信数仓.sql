select * from student;
create table student2(id int, name string);
insert overwrite table student2
select * from student;
show databases ;
use behavior;
use default;
create function get_city_by_ip
    as 'cn.wolfcode.udf.Ip2Loc' using jar 'hdfs://hadoop102:8020//spark-jars/hive_udf_custom-1.0.0.jar';
select  get_city_by_ip("192.168.113.1");

create function url_trans_udf
    as 'cn.wolfcode.udf.UrlHandlerUdf' using jar 'hdfs://hadoop102:8020//spark-jars/hive_udf_custom-1.0.0.jar';

select url_trans_udf("http://www.baidu.com?name=kw");

select * from ods_behavior_log;
select * from dwd_behavior_log;

insert overwrite table dwd_behavior_log partition (dt)
select get_json_object(line, '$.client_ip'),
       get_json_object(line, '$.device_type'),
       get_json_object(line, '$.type'),
       get_json_object(line, '$.device'),
       url_trans_udf(get_json_object(line, '$.url')),
       split(get_city_by_ip(get_json_object(line, '$.client_ip')),"_")[0],
       get_json_object(line, '$.time'),
       dt
from ods_behavior_log;

select * from dwd_behavior_log;
