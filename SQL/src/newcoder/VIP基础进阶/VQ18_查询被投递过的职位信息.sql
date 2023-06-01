/*
drop table if exists deliver_record;
drop table if exists job_info;
CREATE TABLE `deliver_record` (
`user_id` int NOT NULL, -- 投递用户id
`job_id` int NOT NULL, -- 投递职位ID
`platform` varchar(10) NOT NULL, -- 投递平台
`resume_id` int  NOT NULL, -- 投递的简历ID
`resume_if_checked`int NOT NULL, -- 简历是否被查看 1 被查看 0 未被查看
`deliver_date` date  NOT NULL); -- 投递日期

CREATE TABLE `job_info` (
`job_id` int NOT NULL, -- 职位id
`boss_id` int NOT NULL, -- hr id
`company_id` int NOT NULL, -- 公司id
`post_time` datetime NOT NULL, -- 职位发布时间
`salary` int, -- 职位工资
`job_city` varchar(32) NOT NULL ); -- 职位城市
INSERT INTO deliver_record VALUES(101, 18903, 'app', 308897, 1, '2021-03-01');
INSERT INTO deliver_record VALUES(102, 21089, 'pc', 154906, 0, '2022-07-07');
INSERT INTO deliver_record VALUES(102, 22869, 'pc', 967389, 1, '2022-04-09');
INSERT INTO deliver_record VALUES(104, 16739, 'app', 327368, 0, '2018-09-17');
INSERT INTO deliver_record VALUES(105, 34992, 'pc', 600367, 0, '2020-11-15');
INSERT INTO deliver_record VALUES(104, 22889, 'pc', 202819, 1, '2022-05-15');
INSERT INTO job_info VALUES(18903, 202, 3, '2021-03-01 11:22:33', 112000, '北京');
INSERT INTO job_info VALUES(21089, 203, 6, '2022-05-09 09:50:34', 78000, '西安');
INSERT INTO job_info VALUES(22869, 204, 2, '2022-03-09 15:10:50', 92000, '上海');
INSERT INTO job_info VALUES(16739, 204, 6, '2018-08-12 10:00:00', 62000, '杭州');
INSERT INTO job_info VALUES(34992, 205, 9, '2020-11-09 22:01:03', 123000, '北京');
INSERT INTO job_info VALUES(22889, 206, 16, '2022-03-09 01:07:09', 150000, '上海');
commit;
 */

# 简历投递记录
select * from deliver_record;
# 职位信息
select * from job_info;

/*
 现有牛客投递记录表、职位信息表 ，请查询每个公司`company_id`查看过的投递用户数cnt，
 `resume_if_checked` 简历是否被查看 1 被查看 0 未被查看（查看数量为0的不用输出，根据`company_id`升序排序输出）
查询返回结果名称和顺序如下。
 */

select
    t1.company_id,count(if(dr.resume_if_checked = '1',1,null)) cnt
from job_info t1
    left join deliver_record dr on t1.job_id = dr.job_id
group by t1.company_id
having cnt >= 1;
# having 用于分组之后过滤数据。
# 由于筛选条件是在聚合之后的，这时where已不能满足使用，我们就需要用到having了
# 值得注意的是 ：having后面跟的条件判断的字段必须是聚合函数返回的结果，否则sql会报错
