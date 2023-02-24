/*
DROP TABLE IF EXISTS tb_user_video_log, tb_video_info;
CREATE TABLE tb_user_video_log (
id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
uid INT NOT NULL COMMENT '用户ID',
video_id INT NOT NULL COMMENT '视频ID',
start_time datetime COMMENT '开始观看时间',
end_time datetime COMMENT '结束观看时间',
if_follow TINYINT COMMENT '是否关注',
if_like TINYINT COMMENT '是否点赞',
if_retweet TINYINT COMMENT '是否转发',
comment_id INT COMMENT '评论ID'
) CHARACTER SET utf8 COLLATE utf8_bin;

CREATE TABLE tb_video_info (
id INT PRIMARY KEY AUTO_INCREMENT COMMENT '自增ID',
video_id INT UNIQUE NOT NULL COMMENT '视频ID',
author INT NOT NULL COMMENT '创作者ID',
tag VARCHAR(16) NOT NULL COMMENT '类别标签',
duration INT NOT NULL COMMENT '视频时长(秒数)',
release_time datetime NOT NULL COMMENT '发布时间'
)CHARACTER SET utf8 COLLATE utf8_bin;

INSERT INTO tb_user_video_log(uid, video_id, start_time, end_time, if_follow, if_like, if_retweet, comment_id) VALUES
(101, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:20', 0, 1, 1, null)
,(102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:15', 0, 0, 1, null)
,(103, 2001, '2021-10-01 11:00:50', '2021-10-01 11:01:15', 0, 1, 0, 1732526)
,(102, 2002, '2021-09-10 11:00:00', '2021-09-10 11:00:30', 1, 0, 1, null)
,(103, 2002, '2021-10-01 10:59:05', '2021-10-01 11:00:05', 1, 0, 0, null)
,(101, 2003, '2021-09-03 10:00:00', '2021-09-03 10:00:42', 1, 0, 0, null)
,(102, 2003, '2021-09-01 10:00:00', '2021-09-01 10:01:07', 1, 0, 1, null)
;

INSERT INTO tb_video_info(video_id, author, tag, duration, release_time) VALUES
(2001, 901, '影视', 30, '2021-01-01 7:00:00')
,(2002, 901, '影视', 60, '2021-01-01 7:00:00')
,(2003, 902, '旅游', 90, '2020-01-01 7:00:00')
;

 commit;


--------------------------------
 问题：统计在有用户互动的最近一个月（按包含当天在内的近30天算，比如10月31日的近30天为10.2~10.31之间的数据）中，
       每类视频的转发量和转发率（保留3位小数）。

注：转发率＝转发量÷播放量。结果按转发率降序排序。
 */
select * from tb_user_video_log t order by end_time desc ;
select * from tb_video_info;

select
    t1.tag,
    sum(t2.if_retweet) retweet_cut,
    round(sum(t2.if_retweet) / count(t2.if_retweet),3) retweet_rate
from tb_video_info t1
         inner join tb_user_video_log t2 on t1.video_id = t2.video_id
where timestampdiff(day,t2.end_time,(select max(t.end_time) from tb_user_video_log t)) < 30
group by t1.tag
order by retweet_rate desc ;
