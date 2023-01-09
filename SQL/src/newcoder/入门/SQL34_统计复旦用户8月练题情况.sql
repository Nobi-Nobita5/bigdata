#题目信息表
select * from question_detail;
#题目练习情况明细表
select * from question_practice_detail;
#用户信息表
select * from user_profile;

/*
 题目： 现在运营想要了解复旦大学的每个用户在8月份练习的总题目数和回答正确的题目数情况，
        请取出相应明细数据，对于在8月份没有练习过的用户，答题数结果返回0.
 */

select
    t1.device_id,'复旦大学' university,count(t2.question_id) question_cnt,sum(if(t2.result = 'right',1,0)) right_question_cnt
from user_profile t1
    left join question_practice_detail t2
        on t1.device_id = t2.device_id and month(t2.date) = 8
where t1.university = '复旦大学'
group by t1.device_id
;