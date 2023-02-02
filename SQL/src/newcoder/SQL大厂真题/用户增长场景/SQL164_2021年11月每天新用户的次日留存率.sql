/**
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
  (101, 0, '2021-11-01 10:00:00', '2021-11-01 10:00:42', 1),
  (102, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:09', 0),
  (103, 9001, '2021-11-01 10:00:01', '2021-11-01 10:01:50', 0),
  (101, 9002, '2021-11-02 10:00:09', '2021-11-02 10:00:28', 0),
  (103, 9002, '2021-11-02 10:00:51', '2021-11-02 10:00:59', 0),
  (104, 9001, '2021-11-02 10:00:28', '2021-11-02 10:00:50', 0),
  (101, 9003, '2021-11-03 11:00:55', '2021-11-03 11:01:24', 0),
  (104, 9003, '2021-11-03 11:00:45', '2021-11-03 11:00:55', 0),
  (105, 9003, '2021-11-03 11:00:53', '2021-11-03 11:00:59', 0),
  (101, 9002, '2021-11-04 11:00:55', '2021-11-04 11:00:59', 0);
-----------------------
问题：统计2021年11月每天新用户的次日留存率（保留2位小数）

注：
次日留存率为当天新增的用户数中第二天又活跃了的用户数占比。
如果in_time-进入时间和out_time-离开时间跨天了，在两天里都记为该用户活跃过，结果按日期升序。

输出示例：
示例数据的输出结果如下
dt	        uv_left_rate
2021-11-01	0.67
2021-11-02  1.00
2021-11-03  0.00
解释：
11.01有3个用户活跃101、102、103，均为新用户，在11.02只有101、103两个又活跃了，因此11.01的次日留存率为0.67；
11.02有104一位新用户，在11.03又活跃了，因此11.02的次日留存率为1.00；
11.03有105一位新用户，在11.04未活跃，因此11.03的次日留存率为0.00；
11.04没有新用户，不输出。
 */
select * from tb_user_log t;

/**
正确解法1：
1. 先查询出每个用户第一次登陆时间（最小登陆时间）--每天新用户表
2. 因为涉及到跨天活跃，所以要进行并集操作，将登录时间和登出时间取并集，这里union会去重（不保留重复的记录）--用户活跃表
   如果未跨天，去重后，用户活跃表只有一条记录，如果跨天，则有两条记录，代表该用户这两天都活跃。
3. 将每天新用户表和用户活跃表左连接，只有是同一用户并且该用户第2天依旧登陆才会保留整个记录，否则右表记录为空
4. 得到每天新用户第二天是否登陆表后,开始计算每天的次日留存率：根据日期分组计算，次日活跃用户个数/当天新用户个数
 */

select t1.dt,round(count(t2.uid)/count(t1.uid),2) uv_rate
from (select uid
           ,min(date(in_time)) dt
      from tb_user_log
      group by uid) as t1  -- 每天新用户表,用于计算【新用户】的次日留存率
         left join (select uid , date(in_time) dt
                    from tb_user_log
                    union
                    select uid , date(out_time) dt
                    from tb_user_log) as t2 -- 用户活跃表
                   on t1.uid=t2.uid
                       and t1.dt=date_sub(t2.dt,INTERVAL 1 day)
where date_format(t1.dt,'%Y-%m') = '2021-11'
group by t1.dt
order by t1.dt;
