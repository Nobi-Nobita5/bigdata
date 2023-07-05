题目一
题目描述：
请计算在 2022/03/01 - 2022/05/01 内入驻，且行业为制造业、建筑业的企业的基本情况，并将数据插入到新建的表中。
要求输出格式：
日期	一级行业名称	二级行业名称	成功入驻企业量	成功入驻高新技术企业量	企业员工数（万人）
口径说明：
● 制造业：企业的一级行业类目名称为「制造业」
● 建筑业：企业的一级行业类目名称为「建筑业」
新建的表需要设置表的生命周期，表需要有注释，表的字段需要有注释。在答案开始处，请标记 SQL 所适用的数据库 or 平台。

创建表的语法：
  create table [if not exists] table_name
   [(col_name data_type [comment col_comment], …)]
   [comment table_comment]
   [lifecycle days];

底层数据表：
表一：dwd_zmc_ent_settle_di（企业入驻表）
ent_id（PK）	企业 ID	数值类型：BIGINT
settle_time	入驻时间	数值类型：string 格式是：yyyy-mm-dd hh:mi:ss
is_settled	是否已完成入驻	数值类型：string，Y是，N否
is_high_tech	是否高新技术企业	数值类型：string，Y是，N否
employee_cnt	企业员工人数	数值类型：BIGINT，单位人
表二：dim_zmc_ent_info（企业信息维表）
ent_id	企业 ID	数值类型：BIGINT
ent_name	企业名称	数值类型：string
industry_level1_id	行业一级类目 ID	数值类型：BIGINT
industry_level2_id	行业二级类目 ID	数值类型：BIGINT
industry_level1_name	行业一级类目名称	数值类型：string
industry_level2_name	行业二级类目名称	数值类型：string


作答：

--数据库 Mysql
--建表语句
CREATE TABLE IF NOT EXISTS ent_summary
(
  date DATE COMMENT '日期',
  industry_level1_name VARCHAR(255) COMMENT '一级行业名称',
  industry_level2_name VARCHAR(255) COMMENT '二级行业名称',
  settle_enterprises INT COMMENT '成功入驻企业量',
  high_tech_enterprises INT COMMENT '成功入驻高新技术企业量',
  employee_count DECIMAL(10,2) COMMENT '企业员工数（万人）'
) COMMENT '企业基本情况表'
lifecycle 30;

--插入语句
INSERT INTO ent_summary (date,industry_level1_name,industry_level2_name,settle_enterprises,high_tech_enterprises,employee_count)
SELECT
  DATE(settle_time) AS date,
  dwe.industry_level1_name,
  dwe.industry_level2_name,
  COUNT(*) AS settled_enterprises,
  SUM(CASE WHEN is_high_tech = 'Y' THEN 1 ELSE 0 END) AS high_tech_enterprises,
  SUM(employee_cnt) / 10000 AS employee_count
from 
  dwd_zmc_ent_settle_di ds
  JOIN dim_zmc_ent_info dwe on ds.ent_id = dwe.ent_id
where 
  settle_time >= '2022-03-01 00:00:00' and settle_time <= '2022-05-01 23:59:59'
  and (dwe.industry_level1_name = '制造业' OR dwe.industry_level1_name = '建筑业')
group by 
  DATE(settle_time),
  dwe.industry_level1_name,
  dwe.industry_level2_name;



题目二
题目描述：统计状态为在营的优质企业的情况。
要求输出格式：
省份	优质企业数	其中连续评为A级纳税人的企业数	其中高等规模企业数	其中长期经营企业数

口径解释：满足任何以下条件，既可以判定为优质企业：
● 连续评为A级纳税人：过去两年连续被评为 A 级纳税人
● 高等规模企业：最新年报中企业员工人数排名在全部企业的前 10%
● 长期经营：企业经营至今超过 3 年可判定为长期经营

表一：dim_zmc_ent_info（企业信息维表）
ent_id	企业 ID	数值类型：BIGINT
ent_name	企业名称	数值类型：string
operate_from_date	经营期起日	数值类型：string 格式是：yyyy-mm-dd
ent_status	企业经营状态	数值类型：string，枚举值：在营、停业、注销
province	省份	数值类型：string

表二：dwd_zmc_ent_annual_dd （企业年报记录全表）
ent_id	企业ID 	数值类型：BIGINT
year	年份	数值类型：BIGINT
email	企业邮箱	数值类型：STRING
emp_num	员工人数	数值类型：BIGINT
total_profit	利润总额	数值类型：DOUBLE，单位万元

表三：dwd_zmc_ent_alevel_dd （企业A级纳税人评级全量表）
ent_id	企业ID 	数值类型：BIGINT
year	年份	数值类型：BIGINT
is_alevel	是否 A 级纳税人	数值类型：STRING，Y是，N否


作答：

select 
  di.province as 省份,
  sum(
    CASE WHEN 
    ((select is_alevel from dwd_zmc_ent_alevel_dd where year = YEAR(NOW()) - 1) = 'Y'
            and (select is_alevel from dwd_zmc_ent_alevel_dd where year = YEAR(NOW()) - 2) = 'Y')
    or 
     ann.emp_num >= (select 0.9 * MAX(emp_num) from dwd_zmc_ent_annual_dd) 
    or
     YEAR(NOW) - YEAR(di.operate_from_date) >= 3
    then 1 else 0 
    end
  ) as 优质企业数,
  sum(case when (select is_alevel from dwd_zmc_ent_alevel_dd where year = YEAR(NOW()) - 1) = 'Y'
            and (select is_alevel from dwd_zmc_ent_alevel_dd where year = YEAR(NOW()) - 2) = 'Y'
      then 1 else 0 end) as 其中连续评为A级纳税人的企业数,
  sum(case when ann.emp_num >= (select 0.9 * MAX(emp_num) from dwd_zmc_ent_annual_dd) then 1 else 0 end) as 其中高等规模企业数,
  sum(case when YEAR(NOW) - YEAR(di.operate_from_date) >= 3 then 1 else 0 end) as 其中长期经营企业数
from 
  dim_zmc_ent_info di
  left join (
    select ent_id,Max(year) as year,SUM(emp_num) AS total_emp
    from dwd_zmc_ent_annual_dd
    group by ent_id
  ) ann on di.ent_id = ann.ent_id
  left join dwd_zmc_ent_alevel_dd ale on di.ent_id = ale.ent_id
where 
  di.ent_status = '在营'
group by 
  di.province;

