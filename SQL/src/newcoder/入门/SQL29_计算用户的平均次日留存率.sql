/*
create database if not exists Sql_Practice
default character set utf8;

drop table if exists `user_profile`;
drop table if  exists `question_practice_detail`;
drop table if  exists `question_detail`;
CREATE TABLE `user_profile` (
`id` int NOT NULL,
`device_id` int NOT NULL,
`gender` varchar(14) NOT NULL,
`age` int ,
`university` varchar(32) NOT NULL,
`gpa` float,
`active_days_within_30` int ,
`question_cnt` int ,
`answer_cnt` int
);
CREATE TABLE `question_practice_detail` (
`id` int NOT NULL,
`device_id` int NOT NULL,
`question_id`int NOT NULL,
`result` varchar(32) NOT NULL,
`date` date NOT NULL
);
CREATE TABLE `question_detail` (
`id` int NOT NULL,
`question_id`int NOT NULL,
`difficult_level` varchar(32) NOT NULL
);

INSERT INTO user_profile VALUES(1,2138,'male',21,'北京大学',3.4,7,2,12);
INSERT INTO user_profile VALUES(2,3214,'male',null,'复旦大学',4.0,15,5,25);
INSERT INTO user_profile VALUES(3,6543,'female',20,'北京大学',3.2,12,3,30);
INSERT INTO user_profile VALUES(4,2315,'female',23,'浙江大学',3.6,5,1,2);
INSERT INTO user_profile VALUES(5,5432,'male',25,'山东大学',3.8,20,15,70);
INSERT INTO user_profile VALUES(6,2131,'male',28,'山东大学',3.3,15,7,13);
INSERT INTO user_profile VALUES(7,4321,'male',28,'复旦大学',3.6,9,6,52);
INSERT INTO question_practice_detail VALUES(1,2138,111,'wrong','2021-05-03');
INSERT INTO question_practice_detail VALUES(2,3214,112,'wrong','2021-05-09');
INSERT INTO question_practice_detail VALUES(3,3214,113,'wrong','2021-06-15');
INSERT INTO question_practice_detail VALUES(4,6543,111,'right','2021-08-13');
INSERT INTO question_practice_detail VALUES(5,2315,115,'right','2021-08-13');
INSERT INTO question_practice_detail VALUES(6,2315,116,'right','2021-08-14');
INSERT INTO question_practice_detail VALUES(7,2315,117,'wrong','2021-08-15');
INSERT INTO question_practice_detail VALUES(8,3214,112,'wrong','2021-05-09');
INSERT INTO question_practice_detail VALUES(9,3214,113,'wrong','2021-08-15');
INSERT INTO question_practice_detail VALUES(10,6543,111,'right','2021-08-13');
INSERT INTO question_practice_detail VALUES(11,2315,115,'right','2021-08-13');
INSERT INTO question_practice_detail VALUES(12,2315,116,'right','2021-08-14');
INSERT INTO question_practice_detail VALUES(13,2315,117,'wrong','2021-08-15');
INSERT INTO question_practice_detail VALUES(14,3214,112,'wrong','2021-08-16');
INSERT INTO question_practice_detail VALUES(15,3214,113,'wrong','2021-08-18');
INSERT INTO question_practice_detail VALUES(16,6543,111,'right','2021-08-13');
INSERT INTO question_detail VALUES(1,111,'hard');
INSERT INTO question_detail VALUES(2,112,'medium');
INSERT INTO question_detail VALUES(3,113,'easy');
INSERT INTO question_detail VALUES(4,115,'easy');
INSERT INTO question_detail VALUES(5,116,'medium');
INSERT INTO question_detail VALUES(6,117,'easy');
commit;
 */

# 题目：现在运营想要查看用户在某天刷题后第二天还会再来刷题的平均概率。请你取出相应数据。

# 题目明细
select * from question_detail;
# 设备刷题信息表
select * from question_practice_detail;
# 用户信息明细
select * from user_profile;

/*
 解法一：
    question_practice_detail：设备刷题信息表，唯一主键：id
    uniq_id_date：构建用户第二天来了的临时表，question_practice_detail去重device_id, date，主键：device_id, date

    select
        distinct t1.device_id,t1.date date1,t2.date date2
    from
    question_practice_detail t1 left join uniq_id_date t2
    on t1.device_id = t2.device_id and date_add(t1.date, interval 1 day) = t2.date

    用户第二天还来的概率：count(date2) / count(date1)
                        计算第二天还来的概率，不需要同一天的多条数据，所以两个count中的日期都做了去重处理
 */
select count(date2) / count(date1) as avg_ret
from (
         select
             distinct qpd.device_id,
                      qpd.date as date1,
                      uniq_id_date.date as date2
         from question_practice_detail as qpd
                  left join(
                /*DISTINCT关键字用于去除查询结果中的重复行。当使用DISTINCT关键字时，它会应用于整个选择列表（即在SELECT关键字之后的所有列）*/
             select distinct device_id, date
             from question_practice_detail
         ) as uniq_id_date
                           on qpd.device_id=uniq_id_date.device_id
                               and date_add(qpd.date, interval 1 day)=uniq_id_date.date
     ) as id_last_next_date;
/*
 解法二：
    检查date2和date1的日期差是不是为1，是则为1（次日留存了），否则为0（次日未留存），取avg即可得平均概率
 */
select avg(if(datediff(date2, date1)=1, 1, 0)) as avg_ret
from (
         select
             distinct device_id,
                      date as date1,
                      lead(date) over (partition by device_id order by date) as date2
         from (
                  select distinct device_id, date
                  from question_practice_detail
              ) as uniq_id_date
     ) as id_last_next_date;
