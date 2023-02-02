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
  (101, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:11', 0),
  (102, 9001, '2021-11-01 10:00:09', '2021-11-01 10:00:38', 0),
  (103, 9001, '2021-11-01 10:00:28', '2021-11-01 10:00:58', 0),
  (104, 9002, '2021-11-01 11:00:45', '2021-11-01 11:01:11', 0),
  (105, 9001, '2021-11-01 10:00:51', '2021-11-01 10:00:59', 0),
  (106, 9002, '2021-11-01 11:00:55', '2021-11-01 11:01:24', 0),
  (107, 9001, '2021-11-01 10:00:01', '2021-11-01 10:01:50', 0);
-------------------------
场景逻辑说明：artical_id-文章ID代表用户浏览的文章的ID，artical_id-文章ID为0表示用户在非文章内容页（比如App内的列表页、活动页等）。

问题：统计每篇文章最大同一时刻在看人数，如果同一时刻有进入也有离开时，先记录用户数增加再记录减少，结果按最大人数降序。

输出示例：
示例数据的输出结果如下
artical_id	max_uv
9001	    3
9002	    2
解释：10点0分10秒时，有3个用户正在浏览文章9001；11点01分0秒时，有2个用户正在浏览文章9002。
 */
select  * from tb_user_log;

/**
  在此对原表in_time和out_time进行编码，in为观看人数+1， out为观看人数-1，进行两次SELECT联立，并按artical_id升序，时间戳升序：
 */
SELECT
    artical_id, in_time dt, 1 diff
FROM tb_user_log
WHERE artical_id != 0
UNION ALL
SELECT
    artical_id, out_time dt, -1 diff
FROM tb_user_log
WHERE artical_id != 0
ORDER BY 1,2

/**
  统计按时间戳升序的观看人数变化情况
  注意：
    题目要求在瞬时统计时遵循【先进后出】：如果同一时刻有进入也有离开时，先记录用户数增加，再记录减少。
    因此在ORDER BY层面，在遵循dt升序的同时，还要遵循先+1，再-1的原则，即diff DESC：
 */
SELECT
    artical_id,
    dt,
    SUM(diff) OVER(PARTITION BY artical_id ORDER BY dt,diff desc) instant_viewer_cnt
FROM (
         SELECT
             artical_id, in_time dt, 1 diff
         FROM tb_user_log
         WHERE artical_id != 0
         UNION ALL
         SELECT
             artical_id, out_time dt, -1 diff
         FROM tb_user_log
         WHERE artical_id != 0) t1

/**
  聚合求最大值
 */
SELECT
    artical_id,
    MAX(instant_viewer_cnt) max_uv
FROM (
         SELECT
             artical_id,
             SUM(diff) OVER(PARTITION BY artical_id ORDER BY dt, diff DESC) instant_viewer_cnt
         FROM (
                  SELECT
                      artical_id, in_time dt, 1 diff
                  FROM tb_user_log
                  WHERE artical_id != 0
                  UNION ALL
                  SELECT
                      artical_id, out_time dt, -1 diff
                  FROM tb_user_log
                  WHERE artical_id != 0) t1
     ) t2
GROUP BY 1
ORDER BY 2 DESC


