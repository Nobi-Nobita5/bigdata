/*
DROP TABLE IF EXISTS tb_user_log;
CREATE TABLE tb_user_log (
    id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
    uid INT NOT NULL COMMENT '用户ID',
    artical_id INT NOT NULL COMMENT '视频ID',
    in_time datetime COMMENT '进入时间',
    out_time datetime COMMENT '离开时间',
    sign_in TINYINT DEFAULT 0 COMMENT '是否签到'
) CHARACTER SET utf8 COLLATE utf8_bin;

INSERT INTO tb_user_log(uid, artical_id, in_time, out_time, sign_in) VALUES
  (101, 9001, '2021-10-31 10:00:00', '2021-10-31 10:00:09', 0),
  (102, 9001, '2021-10-31 10:00:00', '2021-10-31 10:00:09', 0),
  (101, 0, '2021-11-01 10:00:00', '2021-11-01 10:00:42', 1),
  (102, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:09', 0),
  (108, 9001, '2021-11-01 10:00:01', '2021-11-01 10:01:50', 0),
  (108, 9001, '2021-11-02 10:00:01', '2021-11-02 10:01:50', 0),
  (104, 9001, '2021-11-02 10:00:28', '2021-11-02 10:00:50', 0),
  (106, 9001, '2021-11-02 10:00:28', '2021-11-02 10:00:50', 0),
  (108, 9001, '2021-11-03 10:00:01', '2021-11-03 10:01:50', 0),
  (109, 9002, '2021-11-03 11:00:55', '2021-11-03 11:00:59', 0),
  (104, 9003, '2021-11-03 11:00:45', '2021-11-03 11:00:55', 0),
  (105, 9003, '2021-11-03 11:00:53', '2021-11-03 11:00:59', 0),
  (106, 9003, '2021-11-03 11:00:45', '2021-11-03 11:00:55', 0);
-----------------------------------
问题：统计每天的日活数及新用户占比

注：
新用户占比=当天的新用户数÷当天活跃用户数（日活数）。
如果in_time-进入时间和out_time-离开时间跨天了，在两天里都记为该用户活跃过。
新用户占比保留2位小数，结果按日期升序排序。

输出示例：
示例数据的输出结果如下
dt	        dau	uv_new_ratio
2021-10-30	2	1.00
2021-11-01  3	0.33
2021-11-02  3	0.67
2021-11-03  5	0.40
解释：
2021年10月31日有2个用户活跃，都为新用户，新用户占比1.00；
2021年11月1日有3个用户活跃，其中1个新用户，新用户占比0.33；
*/

select * from tb_user_log;

/*
日活数
新用户数
*/

select t1.dt , dau , round(ifnull(t2.xyhs,0) / dau ,2) uv_new_ratio
from (
/*每天用户活跃表*/
         select dt, count(uid) dau
         from (
                  select uid, date(in_time) dt
                  from tb_user_log
                  union
                  select uid, date(out_time) dt
                  from tb_user_log
              ) t
         group by t.dt
) t1
left join (
/*新用户表*/
    select distinct min(date(in_time)) dt , count(uid) over(partition by min(date(in_time))) xyhs
    from tb_user_log
    group by uid
) t2 on t1.dt = t2.dt
order by t1.dt
;