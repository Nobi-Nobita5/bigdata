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
  (101, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:30', 0, 1, 1, null),
  (102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:24', 0, 0, 1, null),
  (103, 2001, '2021-10-01 11:00:00', '2021-10-01 11:00:34', 0, 1, 0, 1732526),
  (101, 2002, '2021-09-01 10:00:00', '2021-09-01 10:00:42', 1, 0, 1, null),
  (102, 2002, '2021-10-01 11:00:00', '2021-10-01 11:00:30', 1, 0, 1, null);

INSERT INTO tb_video_info(video_id, author, tag, duration, release_time) VALUES
  (2001, 901, '影视', 30, '2021-01-01 7:00:00'),
  (2002, 901, '美食', 60, '2021-01-01 7:00:00'),
  (2003, 902, '旅游', 90, '2021-01-01 7:00:00');

 commit;

 -------------------------------------------------
描述
用户-视频互动表tb_user_video_log

（uid-用户ID, video_id-视频ID, start_time-开始观看时间, end_time-结束观看时间, if_follow-是否关注, if_like-是否点赞, if_retweet-是否转发, comment_id-评论ID）


短视频信息表tb_video_info

（video_id-视频ID, author-创作者ID, tag-类别标签, duration-视频时长（秒）, release_time-发布时间）


问题：计算2021年里有播放记录的每个视频的完播率(结果保留三位小数)，并按完播率降序排序

注：视频完播率是指完成播放次数占总播放次数的比例。简单起见，结束观看时间与开始播放时间的差>=视频时长时，视为完成播放。

输出示例：
示例数据的结果如下：
video_id	avg_comp_play_rate
2001	0.667
2002	0.000
解释：
视频2001在2021年10月有3次播放记录，观看时长分别为30秒、24秒、34秒，视频时长30秒，因此有两次是被认为完成播放了的，故完播率为0.667；
视频2002在2021年9月和10月共2次播放记录，观看时长分别为42秒、30秒，视频时长60秒，故完播率为0.000。
 */

select * from tb_user_video_log;
select * from tb_video_info;

select
     t1.video_id,
                round(avg(if(timestampdiff(second,t2.start_time,t2.end_time) >= t1.duration,1,0)),3) avg_comp_play_rate
from tb_video_info t1
    inner join tb_user_video_log t2 on t1.video_id = t2.video_id
where date_format(t2.start_time,'%Y') = 2021
        and date_format(t2.end_time,'%Y') = 2021
group by t1.video_id
order by avg_comp_play_rate desc ;