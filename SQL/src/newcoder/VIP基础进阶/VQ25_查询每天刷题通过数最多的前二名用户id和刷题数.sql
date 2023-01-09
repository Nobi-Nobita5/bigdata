/*
 drop table if exists questions_pass_record;
CREATE TABLE `questions_pass_record` (
`user_id` int NOT NULL,
`question_type` varchar(32) NOT NULL,
`device` varchar(14) NOT NULL,
`pass_count` int NOT NULL,
`date` date NOT NULL);
INSERT INTO questions_pass_record VALUES(101, 'java', 'app', 2, '2020-03-01');
INSERT INTO questions_pass_record VALUES(102, 'sql', 'pc', 15,'2020-03-01');
INSERT INTO questions_pass_record VALUES(102, 'python', 'pc', 9, '2021-04-09');
INSERT INTO questions_pass_record VALUES(202, 'python', 'pc', 11, '2021-04-09');
INSERT INTO questions_pass_record VALUES(104, 'python', 'app', 3,'2021-04-09');
INSERT INTO questions_pass_record VALUES(105, 'sql', 'pc', 60, '2018-08-15');
INSERT INTO questions_pass_record VALUES(104, 'sql', 'pc', 20, '2018-08-15');
INSERT INTO questions_pass_record VALUES(304, 'sql', 'pc', 10, '2018-08-15');
 */


 select * from questions_pass_record;
# 业务主键：user_id + date

/*
描述
现有牛客刷题表`questions_pass_record`，请查询每天刷题通过数最多的前二名用户id和刷题数，输出按照日期升序排序，
查询返回结果名称和顺序为 date|user_id|pass_count.

解法：
表的业务主键是用户id + 日期。
可以按日期分组，对分组内的pass_count 降序排序。
即可通过筛选rk <= 2得到每天刷题通过数最多的前两名用户
 */

select date,user_id,pass_count from (
                                        select user_id, pass_count, date, row_number() over(partition by date order by pass_count desc) rk
                                        from questions_pass_record
                                    )t1
where rk <=2

