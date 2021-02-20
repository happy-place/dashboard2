show tables;

INSERT INTO dws_space_1d_updated_statistic_by_global_daily VALUES('2020-12-15','37','kBEXadGrG8N0fjVQ','测试空间','2020-11-16 04:02:59');

delete from dws_space_1d_updated_statistic_by_global_daily where ldate = '2020-12-15';


select * from boss.dws_file_7d_product_statistic_by_global_daily where ldate='2020-12-22';

select * from boss.dws_enterprise_7d_user_statistic_by_global_daily where team_id=13;

select count(1) as cnt,'dws_collaboration_7d_statistic_by_department_daily' as tab from boss.dws_collaboration_7d_statistic_by_department_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_collaboration_7d_statistic_by_global_daily' as tab from boss.dws_collaboration_7d_statistic_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_collaboration_7d_statistic_by_member_daily' as tab from boss.dws_collaboration_7d_statistic_by_member_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_collaboration_30d_statistic_by_member_daily' as tab from boss.dws_collaboration_30d_statistic_by_member_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_creation_1d_download_by_global_daily' as tab from boss.dws_creation_1d_download_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_enterprise_7d_user_statistic_by_global_daily' as tab from boss.dws_enterprise_7d_user_statistic_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_enterprise_td_usage_statistic_by_global_daily' as tab from boss.dws_enterprise_td_usage_statistic_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_file_1d_hot_statistic_by_global_daily' as tab from boss.dws_file_1d_hot_statistic_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_file_7d_hot_statistic_by_global_daily' as tab from boss.dws_file_7d_hot_statistic_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_file_7d_product_statistic_by_department_daily' as tab from boss.dws_file_7d_product_statistic_by_department_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_file_7d_product_statistic_by_global_daily' as tab from boss.dws_file_7d_product_statistic_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_file_7d_product_statistic_by_member_daily' as tab from boss.dws_file_7d_product_statistic_by_member_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_file_7d_statistic_by_department_daily' as tab from boss.dws_file_7d_statistic_by_department_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_file_7d_statistic_by_global_daily' as tab from boss.dws_file_7d_statistic_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_file_7d_statistic_by_member_daily' as tab from boss.dws_file_7d_statistic_by_member_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_space_1d_updated_statistic_by_global_daily' as tab from boss.dws_space_1d_updated_statistic_by_global_daily where ldate='2020-12-22' 
union all
select count(1) as cnt,'dws_usage_1d_download_by_member_daily' as tab from boss.dws_usage_1d_download_by_member_daily where ldate='2020-12-22'
order by tab;



select sum(cnt) from ( 
select count(1) as cnt,'dws_collaboration_7d_statistic_by_department_daily' as tab from boss.dws_collaboration_7d_statistic_by_department_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_collaboration_7d_statistic_by_global_daily' as tab from boss.dws_collaboration_7d_statistic_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_collaboration_7d_statistic_by_member_daily' as tab from boss.dws_collaboration_7d_statistic_by_member_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_collaboration_30d_statistic_by_member_daily' as tab from boss.dws_collaboration_30d_statistic_by_member_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_creation_1d_download_by_global_daily' as tab from boss.dws_creation_1d_download_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_enterprise_7d_user_statistic_by_global_daily' as tab from boss.dws_enterprise_7d_user_statistic_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_enterprise_td_usage_statistic_by_global_daily' as tab from boss.dws_enterprise_td_usage_statistic_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_file_1d_hot_statistic_by_global_daily' as tab from boss.dws_file_1d_hot_statistic_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_file_7d_hot_statistic_by_global_daily' as tab from boss.dws_file_7d_hot_statistic_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_file_7d_product_statistic_by_department_daily' as tab from boss.dws_file_7d_product_statistic_by_department_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_file_7d_product_statistic_by_global_daily' as tab from boss.dws_file_7d_product_statistic_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_file_7d_product_statistic_by_member_daily' as tab from boss.dws_file_7d_product_statistic_by_member_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_file_7d_statistic_by_department_daily' as tab from boss.dws_file_7d_statistic_by_department_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_file_7d_statistic_by_global_daily' as tab from boss.dws_file_7d_statistic_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_file_7d_statistic_by_member_daily' as tab from boss.dws_file_7d_statistic_by_member_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_space_1d_updated_statistic_by_global_daily' as tab from boss.dws_space_1d_updated_statistic_by_global_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
union all 
select count(1) as cnt,'dws_usage_1d_download_by_member_daily' as tab from boss.dws_usage_1d_download_by_member_daily where ldate >='2020-12-15' and ldate <='2020-12-23' 
) temp ;



select * from boss.dws_file_7d_statistic_by_global_daily where ldate = '2021-01-11' and team_id='183115';

select * from boss.dws_file_7d_product_statistic_by_global_daily where ldate = '2021-01-11' and team_id='183115';


;


