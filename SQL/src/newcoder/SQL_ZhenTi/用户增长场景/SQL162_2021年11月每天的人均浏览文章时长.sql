/**
---------------------
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
  (101, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:31', 0),
  (102, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:24', 0),
  (102, 9002, '2021-11-01 11:00:00', '2021-11-01 11:00:11', 0),
  (101, 9001, '2021-11-02 10:00:00', '2021-11-02 10:00:50', 0),
  (102, 9002, '2021-11-02 11:00:01', '2021-11-02 11:00:24', 0);
------------------
场景逻辑说明：artical_id-文章ID代表用户浏览的文章的ID，artical_id-文章ID为0表示用户在非文章内容页（比如App内的列表页、活动页等）。

问题：统计2021年11月每天的人均浏览文章时长（秒数），结果保留1位小数，并按时长由短到长排序。

输出示例：
示例数据的输出结果如下
dt	        avg_viiew_len_sec
2021-11-01	33.0
2021-11-02	36.5
解释：
11月1日有2个人浏览文章，总共浏览时长为31+24+11=66秒，人均浏览33秒；
11月2日有2个人浏览文章，总共时长为50+23=73秒，人均时长为36.5秒。
 */

select timestampdiff(second,in_time,out_time) from tb_user_log where id = 1;

select date_format(t.in_time,'%Y-%m-%d') dt,round(sum(timestampdiff(second,in_time,out_time)) / count(distinct t.uid),1) avg_viiew_len_sec
from tb_user_log t
where date_format(t.in_time,'%Y-%m') = '2021-11' and t.artical_id != 0
group by dt
order by avg_viiew_len_sec asc