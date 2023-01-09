#题目信息表
select * from question_detail;
#题目练习情况明细表
select * from question_practice_detail;
#用户信息表
select * from user_profile;

/*
 题目：现在运营想要了解浙江大学的用户在不同难度题目下答题的正确率情况，
       请取出相应数据，并按照准确率升序输出。
 */

# 正确的题数可以用count (使用null)、sum
# 正确率的计算也可以直接使用avg
select
    t3.difficult_level,
       avg(if(t2.result = 'right',1,0)) ,
       count(if(t2.result = 'right',1,null)) / count(t2.question_id),
       sum(if(t2.result = 'right',1,0)) / count(t2.question_id) correct_rate
from user_profile t1
    left join question_practice_detail t2
        on t1.device_id = t2.device_id
    left join question_detail t3
        on t2.question_id = t3.question_id
where t1.university = '浙江大学'
group by t3.difficult_level
order by correct_rate asc;
