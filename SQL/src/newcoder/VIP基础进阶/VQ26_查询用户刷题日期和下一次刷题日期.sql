/*
 drop table if exists questions_pass_record;
CREATE TABLE `questions_pass_record` (
`user_id` int NOT NULL,
`question_type` varchar(32) NOT NULL,
`device` varchar(14) NOT NULL,
`question_id` int NOT NULL,
`date` date NOT NULL);
INSERT INTO questions_pass_record VALUES(101, 'java', 'app', 2, '2020-03-01');
INSERT INTO questions_pass_record VALUES(102, 'sql', 'pc', 15,'2021-07-07');
INSERT INTO questions_pass_record VALUES(102, 'python', 'pc', 9, '2021-04-09');
INSERT INTO questions_pass_record VALUES(102, 'python', 'app', 3,'2022-03-17');
INSERT INTO questions_pass_record VALUES(103, 'sql', 'pc', 60, '2018-08-15');
INSERT INTO questions_pass_record VALUES(104, 'sql', 'pc', 20, '2019-05-15');
INSERT INTO questions_pass_record VALUES(105, 'sql', 'pc', 550, '2022-07-25');
INSERT INTO questions_pass_record VALUES(105, 'sql', 'pc', 299, '2020-05-16');
INSERT INTO questions_pass_record VALUES(106, 'java', 'pc', 17, '2021-04-15');
INSERT INTO questions_pass_record VALUES(106, 'java', 'pc', 20, '2021-04-15');
commit;
 */

 select * from questions_pass_record;
 # 业务主键：user_id + date
 /*
描述
现有牛客刷题记录表`questions_pass_record` ，请查询用户user_id，刷题日期date （每组按照date降序排列）
和该用户的下一次刷题日期nextdate（若是没有则为None），组之间按照user_id升序排序，每组内按照date升序排列，
查询返回结果名称和顺序为 user_id|date|nextdate
  */


select user_id,date,
       lead(date,1,'None') over (partition by user_id  order by date asc) nextdate
from questions_pass_record;