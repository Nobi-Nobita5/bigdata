��Ŀһ
��Ŀ������
������� 2022/03/01 - 2022/05/01 ����פ������ҵΪ����ҵ������ҵ����ҵ�Ļ���������������ݲ��뵽�½��ı��С�
Ҫ�������ʽ��
����	һ����ҵ����	������ҵ����	�ɹ���פ��ҵ��	�ɹ���פ���¼�����ҵ��	��ҵԱ���������ˣ�
�ھ�˵����
�� ����ҵ����ҵ��һ����ҵ��Ŀ����Ϊ������ҵ��
�� ����ҵ����ҵ��һ����ҵ��Ŀ����Ϊ������ҵ��
�½��ı���Ҫ���ñ���������ڣ�����Ҫ��ע�ͣ�����ֶ���Ҫ��ע�͡��ڴ𰸿�ʼ�������� SQL �����õ����ݿ� or ƽ̨��

��������﷨��
  create table [if not exists] table_name
   [(col_name data_type [comment col_comment], ��)]
   [comment table_comment]
   [lifecycle days];

�ײ����ݱ�
��һ��dwd_zmc_ent_settle_di����ҵ��פ��
ent_id��PK��	��ҵ ID	��ֵ���ͣ�BIGINT
settle_time	��פʱ��	��ֵ���ͣ�string ��ʽ�ǣ�yyyy-mm-dd hh:mi:ss
is_settled	�Ƿ��������פ	��ֵ���ͣ�string��Y�ǣ�N��
is_high_tech	�Ƿ���¼�����ҵ	��ֵ���ͣ�string��Y�ǣ�N��
employee_cnt	��ҵԱ������	��ֵ���ͣ�BIGINT����λ��
�����dim_zmc_ent_info����ҵ��Ϣά��
ent_id	��ҵ ID	��ֵ���ͣ�BIGINT
ent_name	��ҵ����	��ֵ���ͣ�string
industry_level1_id	��ҵһ����Ŀ ID	��ֵ���ͣ�BIGINT
industry_level2_id	��ҵ������Ŀ ID	��ֵ���ͣ�BIGINT
industry_level1_name	��ҵһ����Ŀ����	��ֵ���ͣ�string
industry_level2_name	��ҵ������Ŀ����	��ֵ���ͣ�string


����

--���ݿ� Mysql
--�������
CREATE TABLE IF NOT EXISTS ent_summary
(
  date DATE COMMENT '����',
  industry_level1_name VARCHAR(255) COMMENT 'һ����ҵ����',
  industry_level2_name VARCHAR(255) COMMENT '������ҵ����',
  settle_enterprises INT COMMENT '�ɹ���פ��ҵ��',
  high_tech_enterprises INT COMMENT '�ɹ���פ���¼�����ҵ��',
  employee_count DECIMAL(10,2) COMMENT '��ҵԱ���������ˣ�'
) COMMENT '��ҵ���������'
lifecycle 30;

--�������
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
  and (dwe.industry_level1_name = '����ҵ' OR dwe.industry_level1_name = '����ҵ')
group by 
  DATE(settle_time),
  dwe.industry_level1_name,
  dwe.industry_level2_name;



��Ŀ��
��Ŀ������ͳ��״̬Ϊ��Ӫ��������ҵ�������
Ҫ�������ʽ��
ʡ��	������ҵ��	����������ΪA����˰�˵���ҵ��	���иߵȹ�ģ��ҵ��	���г��ھ�Ӫ��ҵ��

�ھ����ͣ������κ������������ȿ����ж�Ϊ������ҵ��
�� ������ΪA����˰�ˣ���ȥ������������Ϊ A ����˰��
�� �ߵȹ�ģ��ҵ�������걨����ҵԱ������������ȫ����ҵ��ǰ 10%
�� ���ھ�Ӫ����ҵ��Ӫ���񳬹� 3 ����ж�Ϊ���ھ�Ӫ

��һ��dim_zmc_ent_info����ҵ��Ϣά��
ent_id	��ҵ ID	��ֵ���ͣ�BIGINT
ent_name	��ҵ����	��ֵ���ͣ�string
operate_from_date	��Ӫ������	��ֵ���ͣ�string ��ʽ�ǣ�yyyy-mm-dd
ent_status	��ҵ��Ӫ״̬	��ֵ���ͣ�string��ö��ֵ����Ӫ��ͣҵ��ע��
province	ʡ��	��ֵ���ͣ�string

�����dwd_zmc_ent_annual_dd ����ҵ�걨��¼ȫ��
ent_id	��ҵID 	��ֵ���ͣ�BIGINT
year	���	��ֵ���ͣ�BIGINT
email	��ҵ����	��ֵ���ͣ�STRING
emp_num	Ա������	��ֵ���ͣ�BIGINT
total_profit	�����ܶ�	��ֵ���ͣ�DOUBLE����λ��Ԫ

������dwd_zmc_ent_alevel_dd ����ҵA����˰������ȫ����
ent_id	��ҵID 	��ֵ���ͣ�BIGINT
year	���	��ֵ���ͣ�BIGINT
is_alevel	�Ƿ� A ����˰��	��ֵ���ͣ�STRING��Y�ǣ�N��


����

select 
  di.province as ʡ��,
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
  ) as ������ҵ��,
  sum(case when (select is_alevel from dwd_zmc_ent_alevel_dd where year = YEAR(NOW()) - 1) = 'Y'
            and (select is_alevel from dwd_zmc_ent_alevel_dd where year = YEAR(NOW()) - 2) = 'Y'
      then 1 else 0 end) as ����������ΪA����˰�˵���ҵ��,
  sum(case when ann.emp_num >= (select 0.9 * MAX(emp_num) from dwd_zmc_ent_annual_dd) then 1 else 0 end) as ���иߵȹ�ģ��ҵ��,
  sum(case when YEAR(NOW) - YEAR(di.operate_from_date) >= 3 then 1 else 0 end) as ���г��ھ�Ӫ��ҵ��
from 
  dim_zmc_ent_info di
  left join (
    select ent_id,Max(year) as year,SUM(emp_num) AS total_emp
    from dwd_zmc_ent_annual_dd
    group by ent_id
  ) ann on di.ent_id = ann.ent_id
  left join dwd_zmc_ent_alevel_dd ale on di.ent_id = ale.ent_id
where 
  di.ent_status = '��Ӫ'
group by 
  di.province;

