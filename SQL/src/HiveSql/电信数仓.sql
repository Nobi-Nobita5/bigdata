show databases ;
use behavior;
use default;

select *  from ods_behavior_log;

CREATE EXTERNAL TABLE dwd_behavior_log
(
    `client_ip`   STRING COMMENT '客户端IP',
    `device_type` STRING COMMENT '设备类型',
    `type`        STRING COMMENT '上网类型 4G 5G WiFi',
    `device`      STRING COMMENT '设备ID',
    `url`         STRING COMMENT '访问的资源路径',
    `city`        STRING COMMENT '城市',
    `ts`          bigint comment "时间戳"
) COMMENT '页面启动日志表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/behavior/dwd/dwd_behavior_log'
    TBLPROPERTIES ("orc.compress" = "snappy");


