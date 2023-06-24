/**
拼多多：
sql题目：输出表中num字段最长的连续递增序列。
表t如下：
id num
1		2
2		3
3		4
4		3
5		4
6		5
7		6
8		4
9		5
应该输出：3 4 5 6
写出这个sql
 */

WITH
    LagQuery AS (
        SELECT
            t.*,
            IF(t.num > LAG(t.num) OVER (ORDER BY t.id), 0, 1) AS isNewSeq
        FROM t
    ),
    GroupQuery AS (
        SELECT
            l.*,
            SUM(l.isNewSeq) OVER (ORDER BY l.id) AS groupId
        FROM LagQuery l
    ),
    GroupCount as (
        select
            a.groupId,count(*) count_group
        from
        GroupQuery a
        group by a.groupId
    ),
    maxGroup as(
        select
            t.groupId,t.count_group
        from
        GroupCount t
        order by t.count_group desc
        limit 1
    )
    select t1.num
        from
            GroupQuery t1
        inner join maxGroup t2 on t1.groupId = t2.groupId
        order by t1.id;

