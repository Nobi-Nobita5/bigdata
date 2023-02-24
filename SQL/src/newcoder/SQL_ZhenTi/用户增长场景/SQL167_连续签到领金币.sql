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
  (101, 0, '2021-07-07 10:00:00', '2021-07-07 10:00:09', 1),
  (101, 0, '2021-07-08 10:00:00', '2021-07-08 10:00:09', 1),
  (101, 0, '2021-07-09 10:00:00', '2021-07-09 10:00:42', 1),
  (101, 0, '2021-07-10 10:00:00', '2021-07-10 10:00:09', 1),
  (101, 0, '2021-07-11 23:59:55', '2021-07-11 23:59:59', 1),
  (101, 0, '2021-07-12 10:00:28', '2021-07-12 10:00:50', 1),
  (101, 0, '2021-07-13 10:00:28', '2021-07-13 10:00:50', 1),
  (102, 0, '2021-10-01 10:00:28', '2021-10-01 10:00:50', 1),
  (102, 0, '2021-10-02 10:00:01', '2021-10-02 10:01:50', 1),
  (102, 0, '2021-10-03 11:00:55', '2021-10-03 11:00:59', 1),
  (102, 0, '2021-10-04 11:00:45', '2021-10-04 11:00:55', 0),
  (102, 0, '2021-10-05 11:00:53', '2021-10-05 11:00:59', 1),
  (102, 0, '2021-10-06 11:00:45', '2021-10-06 11:00:55', 1);
----------------------------------
场景逻辑说明：
artical_id-文章ID代表用户浏览的文章的ID，特殊情况artical_id-文章ID为0表示用户在非文章内容页（比如App内的列表页、活动页等）。注意：只有artical_id为0时sign_in值才有效。
从2021年7月7日0点开始，用户每天签到可以领1金币，并可以开始累积签到天数，连续签到的第3、7天分别可额外领2、6金币。
每连续签到7天后重新累积签到天数（即重置签到天数：连续第8天签到时记为新的一轮签到的第一天，领1金币）

问题：计算每个用户2021年7月以来每月获得的金币数（该活动到10月底结束，11月1日开始的签到不再获得金币）。结果按月份、ID升序排序。

注：如果签到记录的in_time-进入时间和out_time-离开时间跨天了，也只记作in_time对应的日期签到了。

输出示例：
示例数据的输出结果如下：
uid	month	coin
101	202107	15
102	202110	7
解释：
101在活动期内连续签到了7天，因此获得1*7+2+6=15金币；
102在10.01~10.03连续签到3天获得5金币
10.04断签了，10.05~10.06连续签到2天获得2金币，共得到7金币。
*/

select * from tb_user_log;


/*
比较好理解的思考方式是根据需要的结果，一步一步反推自己需要什么的格式的数据
1.要求活动期间的签到获得的金币总数，那我最希望的是能够获得每一天用户签到时获得的金币数，然后只需要按照ID和month分组，sum一下就可以
2.再反推，想要获得每一天用户签到时获得的金币数，那么我必须知道，用户当天签到是连续签到的第几天，得到天数以后很简单了，用case when 将天数 % 7 ，看余数。 余数是3 ，当天获得 3枚。余数是 0 ，当天获得7枚 。其他为 1 枚 。
3.推到这里那其实思路已经清晰了，求连续签到的天数，那无非就是连续问题了:
    对于同一用户，因为排序编号和签到日期的差值是相等的。用dt_tmp存放排序编号和签到日期的差值。
    那么按，uid和dt_tmp分组，再dense_rank就可以得到连续签到的天数。
*/
WITH t1 AS( -- t1表筛选出活动期间内的数据，并且为了防止一天有多次签到活动，distinct 去重
    SELECT
        DISTINCT uid,
                 DATE(in_time) dt,
                 DENSE_RANK() over(PARTITION BY uid ORDER BY DATE(in_time)) rn -- 编号
    FROM
        tb_user_log
    WHERE
        DATE(in_time) BETWEEN '2021-07-07' AND '2021-10-31' AND artical_id = 0 AND sign_in = 1
),
     t2 AS (
         SELECT
             *,
             DATE_SUB(dt,INTERVAL rn day) dt_tmp,
             case DENSE_RANK() over(PARTITION BY DATE_SUB(dt,INTERVAL rn day),uid ORDER BY dt )%7 -- 再次编号
                 WHEN 3 THEN 3
                 WHEN 0 THEN 7
                 ELSE 1
                 END as day_coin -- 用户当天签到时应该获得的金币数
         FROM
             t1
     )
SELECT
    uid,DATE_FORMAT(dt,'%Y%m') `month`, sum(day_coin) coin  -- 总金币数
FROM
    t2
GROUP BY
    uid,DATE_FORMAT(dt,'%Y%m')
ORDER BY
    DATE_FORMAT(dt,'%Y%m'),uid;
