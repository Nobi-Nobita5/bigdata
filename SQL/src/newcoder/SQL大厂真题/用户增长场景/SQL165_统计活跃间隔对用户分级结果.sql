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
  (109, 9001, '2021-08-31 10:00:00', '2021-08-31 10:00:09', 0),
  (109, 9002, '2021-11-04 11:00:55', '2021-11-04 11:00:59', 0),
  (108, 9001, '2021-09-01 10:00:01', '2021-09-01 10:01:50', 0),
  (108, 9001, '2021-11-03 10:00:01', '2021-11-03 10:01:50', 0),
  (104, 9001, '2021-11-02 10:00:28', '2021-11-02 10:00:50', 0),
  (104, 9003, '2021-09-03 11:00:45', '2021-09-03 11:00:55', 0),
  (105, 9003, '2021-11-03 11:00:53', '2021-11-03 11:00:59', 0),
  (102, 9001, '2021-10-30 10:00:00', '2021-10-30 10:00:09', 0),
  (103, 9001, '2021-10-21 10:00:00', '2021-10-21 10:00:09', 0),
  (101, 0, '2021-10-01 10:00:00', '2021-10-01 10:00:42', 1);
--------------------------------------------
问题：统计活跃间隔对用户分级后，各活跃等级用户占比，结果保留两位小数，且按占比降序排序。

注：
用户等级标准简化为：忠实用户(近7天活跃过且非新晋用户)、新晋用户(近7天新增)、沉睡用户(近7天未活跃但更早前活跃过)、流失用户(近30天未活跃但更早前活跃过)。
假设今天就是数据中所有日期的最大值。
近7天表示包含当天T的近7天，即闭区间[T-6, T]。

输出示例：
示例数据的输出结果如下
user_grade	ratio
忠实用户	    0.43
新晋用户	    0.29
沉睡用户	    0.14
流失用户	    0.14
解释：
今天日期为2021.11.04，根据用户分级标准，用户行为日志表tb_user_log中忠实用户有：109、108、104；新晋用户有105、102；沉睡用户有103；流失用户有101；
共7个用户，因此他们的比例分别为0.43、0.29、0.14、0.14。
 */

select * from tb_user_log;

/*
活跃等级：根据【最早活跃时间、最晚活跃时间 ~ 今天】判断
各活跃等级用户占比：该等级人数 / 总人数

根据需要构建中间表
*/
SELECT user_grade, ROUND(COUNT(uid) / MAX(user_cnt), 2) as ratio
FROM (
         SELECT uid, user_cnt,
                CASE
                    WHEN last_dt_diff >= 30 THEN "流失用户"
                    WHEN last_dt_diff >= 7 THEN "沉睡用户"
                    WHEN first_dt_diff < 7 THEN "新晋用户" /*【最早活跃时间-今天】小于7，为新晋用户*/
                    ELSE "忠实用户"
                    END as user_grade
         FROM (
                  SELECT uid, user_cnt,
                         TIMESTAMPDIFF(DAY,first_dt,cur_dt) as first_dt_diff, /*【最早活跃时间-今天】*/
                         TIMESTAMPDIFF(DAY,last_dt,cur_dt) as last_dt_diff /*【最晚活跃时间-今天】*/
                  FROM (
                           SELECT uid, MIN(DATE(in_time)) as first_dt,
                                  MAX(DATE(out_time)) as last_dt
                           FROM tb_user_log
                           GROUP BY uid
                       ) as t_uid_first_last  /*记录每个用户的最早活跃时间和最晚活跃时间*/
                           LEFT JOIN (
                      SELECT MAX(DATE(out_time)) as cur_dt,
                             COUNT(DISTINCT uid) as user_cnt
                      FROM tb_user_log
                  ) as t_overall_info ON 1 /*只有一条记录，记录总人数和今天的日期，on 1 相当于 on 1=1*/
              ) as t_user_info
     ) as t_user_grade
GROUP BY user_grade
ORDER BY ratio DESC;
