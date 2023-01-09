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
(102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:21', 0, 0, 1, null),
(103, 2001, '2021-10-01 11:00:50', '2021-10-01 11:01:25', 0, 1, 0, 1732526),
(102, 2002, '2021-10-01 11:00:00', '2021-10-01 11:00:30', 1, 0, 1, null),
(103, 2002, '2021-10-01 10:59:05', '2021-10-01 11:00:05', 1, 0, 1, null),
(101, 2003, '2020-09-01 10:00:00', '2020-09-01 10:00:42', 1, 0, 1, null),
(102, 2003, '2021-09-01 10:00:00', '2021-09-01 10:01:06', 1, 0, 1, null);

INSERT INTO tb_video_info(video_id, author, tag, duration, release_time) VALUES
(2001, 901, '影视', 30, '2021-01-01 7:00:00'),
(2002, 901, '美食', 60, '2021-01-01 7:00:00'),
(2003, 902, '旅游', 90, '2020-01-01 7:00:00');
commit;

问题：计算各类视频的平均播放进度，将进度大于60%的类别输出。

注：
播放进度=播放时长÷视频时长*100%，当播放时长大于视频时长时，播放进度均记为100%。
结果保留两位小数，并按播放进度倒序排序。

输出示例：
示例数据的输出结果如下：
tag	avg_play_progress
影视  90.00%
美食  75.00%
解释：
影视类视频2001被用户101、102、103看过，播放进度分别为：30秒（100%）、21秒（70%）、30秒（100%），平均播放进度为90.00%（保留两位小数）；
美食类视频2002被用户102、103看过，播放进度分别为：30秒（50%）、60秒（100%），平均播放进度为75.00%（保留两位小数）；
 */
select * from tb_user_video_log;
select * from tb_video_info;

select
    t1.tag,
    concat(
            round(
                  sum(
                      case
                          when timestampdiff(second, t2.start_time, t2.end_time) > t1.duration then t1.duration
                          else timestampdiff(second, t2.start_time, t2.end_time)
                          end
                      ) / sum(t1.duration) * 100,
                  2
                ),
            '%'
        ) avg_play_progress
from
    tb_video_info t1
        inner join tb_user_video_log t2 on t1.video_id = t2.video_id
group by t1.tag
having avg_play_progress > 60
order by avg_play_progress desc;
