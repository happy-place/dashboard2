select
     '2020-12-14' as ldate,
        team_id,
       sum(create_objs) as create_objs,
       sum(create_docxs) as create_docxs,
       sum(create_sheets) as create_sheets,
       sum(create_tables) as create_tables,
       sum(create_ppts) as create_ppts,
       sum(create_docs) as create_docs,
       sum(create_clouds) as create_clouds,
       sum(create_others) as create_others,
       sum(file_views) as file_views,
       sum(add_collaborations) as add_collaborations,
       sum(use_ats) as use_ats,
       sum(public_shares) as public_shares,
       sum(comments) as comments
from (
     select team_id,
            0               as create_objs,
            0               as create_docxs,
            0               as create_sheets,
            0               as create_tables,
            0               as create_ppts,
            0               as create_docs,
            0               as create_clouds,
            0               as create_others,
            sum(file_views) as file_views,
            0               as add_collaborations,
            0               as use_ats,
            0               as public_shares,
            0               as comments
     from cdm.saas_td_file_views
     where ldate <= '2020-12-14'
     group by team_id

     union all

     select team_id,
            sum(create_objs)   as create_objs,
            sum(create_docxs)  as create_docxs,
            sum(create_sheets) as create_sheets,
            sum(create_tables) as create_tables,
            sum(create_ppts)   as create_ppts,
            sum(create_docs)   as create_docs,
            sum(create_clouds) as create_clouds,
            sum(create_others) as create_others,
            0                  as file_views,
            0                  as add_collaborations,
            0                  as use_ats,
            0                  as public_shares,
            0                  as comments
     from cdm.saas_td_file_create_by_product
     where ldate = '2020-12-14'
     group by team_id

     union all

     select team_id,
            0                       as create_objs,
            0                       as create_docxs,
            0                       as create_sheets,
            0                       as create_tables,
            0                       as create_ppts,
            0                       as create_docs,
            0                       as create_clouds,
            0                       as create_others,
            0                       as file_views,
            sum(add_collaborations) as add_collaborations,
            sum(use_ats)            as use_ats,
            sum(public_shares)      as public_shares,
            sum(comments)           as comments
     from cdm.saas_td_collaboration
     where ldate <= '2020-12-14'
     group by team_id
 ) temp group by team_id;