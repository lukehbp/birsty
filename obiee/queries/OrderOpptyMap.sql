select
   oh.header_id order_header_id
  ,oh.header_id order_header_map_id
  ,oh.order_number
  --,oh.attribute11 opportunity_id
  ,REGEXP_REPLACE(oh.attribute11,'(^[[:space:]]*|[[:space:]]*$)') opportunity_id
  ,salesrep.salesrep_id
  ,salesrep.name salesrep_name
  FROM apps.oe_order_headers_all oh
 
          INNER JOIN
          (SELECT DISTINCT
                  rep.salesrep_id, NVL (rep.name, per.full_name) name
             FROM apps.jtf_rs_groups_tl rg
                  INNER JOIN apps.jtf_rs_group_members rgm
                     ON     rg.GROUP_ID = rgm.GROUP_ID
                        AND rg.group_name LIKE 'CL%'
                        AND rgm.delete_flag = 'N'
                  INNER JOIN apps.jtf_rs_salesreps rep
                     ON     rep.resource_id = rgm.resource_id
                        AND rep.salesrep_id <> 100011082
                  INNER JOIN APPS.PER_ALL_PEOPLE_f per
                     ON     per.person_id = rep.person_id
                        AND SYSDATE BETWEEN per.effective_start_date
                                        AND NVL (per.effective_end_date,
                                                 SYSDATE + 1)) salesrep
             ON salesrep.salesrep_id = oh.salesrep_id
where
  oh.attribute11 IS NOT NULL
order by 1 asc