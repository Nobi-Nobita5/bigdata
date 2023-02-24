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
   (101, 2001, '2021-09-01 10:00:00', '2021-09-01 10:00:20', 0, 1, 1, null)
  ,(105, 2002, '2021-09-10 11:00:00', '2021-09-10 11:00:30', 1, 0, 1, null)
  ,(101, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:20', 1, 1, 1, null)
  ,(102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:15', 0, 0, 1, null)
  ,(103, 2001, '2021-10-01 11:00:50', '2021-10-01 11:01:15', 1, 1, 0, 1732526)
  ,(106, 2002, '2021-10-01 10:59:05', '2021-10-01 11:00:05', 2, 0, 0, null);

INSERT INTO tb_video_info(video_id, author, tag, duration, release_time) VALUES
   (2001, 901, '影视', 30, '2021-01-01 7:00:00')
  ,(2002, 901, '影视', 60, '2021-01-01 7:00:00')
  ,(2003, 902, '旅游', 90, '2020-01-01 7:00:00')
  ,(2004, 902, '美女', 90, '2020-01-01 8:00:00');
 commit;

 --------------
问题：计算2021年里每个创作者每月的涨粉率及截止当月的总粉丝量

注：
涨粉率=(加粉量 - 掉粉量) / 播放量。结果按创作者ID、总粉丝量升序排序。
if_follow-是否关注为1表示用户观看视频中关注了视频创作者，
为0表示此次互动前后关注状态未发生变化，
为2表示本次观看过程中取消了关注。
 */

select * from tb_user_video_log ;
select * from tb_video_info;

/*
 题解：
 1.窗口函数中sum(a) over(order by b) 可以按b顺序依次相加a，求得总粉丝量
 2.窗口函数可以和group by 一起使用，只要窗口函数涉及字段满足group by表达式
 */
select t1.author,
       date_format(t2.start_time,'%Y-%m') month,
       round(sum(case when if_follow=1 then 1
                when if_follow=2 then -1
                else 0 end) / count(t2.video_id),3) fans_growth_rate,
       sum(sum(case when t2.if_follow = 1 then 1
                    when t2.if_follow = 0 then 0
                    when t2.if_follow = 2 then -1
           end)) over (partition by t1.author order by date_format(t2.start_time,'%Y-%m')) total_fans
from tb_video_info t1
         left join tb_user_video_log t2 on t1.video_id = t2.video_id
where date_format(t2.start_time, '%Y') = 2021
group by author,month
order by author,total_fans;