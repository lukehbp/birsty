select
  -- cc.segment1
  --,cc.segment1_desc
  --,cc.segment2
  --,cc.segment2_desc
   prd.product_number
  ,prd.format
  ,prd.language
  ,prd.product_number || '-' || prd.format || '-' || prd.language product_number_concat
  ,prd.title_abbreviation
  ,pd.bill_to_party_id
  ,pa.party_name
  ,pa.party_number partynumber_aka_registryid
  ,cb.cust_account_id bill_to_account_id
  ,cb.account_name bill_to_account_name
  ,cb.account_number bill_to_account_number
  ,cb.parent_number parentnumber_aka_parentregid
  ,cb.parent_name bill_to_parent_name
  ,cs.cust_account_id sold_to_account_id
  ,cs.account_name sold_to_account_name
  ,cs.account_number sold_to_account_number
  ,dt.fiscal_yyyymm
  ,dt.date_field "Transaction Date"
  ,7 "Revenue Type ID" -- TCV Contracted
  ,pd.order_header_id
  ,plr.RESOURCE_NAME as "ebs_rep"
  ,sum(pd.open_order_amount) as "Open Order Amount"
  ,sum(pd.closed_order_amount) as "Closed Order Amount"
  -- Zero out TCV values of Orders that are being double counted because they have already been invoiced - EBS Issue)
  ,(CASE
    WHEN (bill_to_party_id in ('60349','283465280','60349','283465280','182701979','5926','282256367'
,'37395','283025694','282462477','62154','52205','55941','281708419','5890','5890','282124891','281275735','10066443','38456215','186298') 
         AND dt.date_field=TO_DATE('2016-10-20', 'YYYY-MM-DD')) THEN 0
    WHEN (bill_to_party_id in ('10405506') 
         AND dt.date_field=TO_DATE('2016-08-17', 'YYYY-MM-DD')) THEN 0
    WHEN pd.order_header_id in ('293382838') THEN 0
    ELSE sum(pd.closed_order_amount) + sum(pd.open_order_amount)
  END) as "Amount"
  from pipeline_detail pd
    --inner join gl_code_combos cc
      --on pd.code_combination_id=cc.code_combination_id
    inner join dim_dates_v dt
      on pd.transaction_date_key=dt.date_key
    inner join products prd
      on pd.inventory_item_id=prd.inventory_item_id
    inner join pipeline_sources ps
      on pd.pipeline_source=ps.pipeline_source
    left outer join parties pa
      on pd.bill_to_party_id=pa.party_id
    left outer join pipeline_resources pr
      on pd.resource_id=pr.resource_id
    left outer join customers_bill_to cb
      on pd.bill_to_customer_id=cb.cust_account_id
    left outer join customers_sold_to cs
      on pd.sold_to_customer_id=cs.cust_account_id
    left outer join pipeline_headers ph
      on pd.pipeline_header_id=ph.pipeline_header_id
      and pd.pipeline_source=ph.pipeline_source
	left join pipeline_resources plr
	  on pd.RESOURCE_ID = plr.RESOURCE_ID
  where dt.fiscal_yyyymm >='201401' --'201401' --'201601' --'201703'
    --and rownum < 10 
    --and cc.segment1='01'
    --and cc.segment2='610'
    and dt.fiscal_yyyymm <='205001'
    and ps.description IN ('Open Order', 'Closed Order')
    and ph.transaction_no not like '9%' -- exclude pro-forma orders
    and pr.resource_id NOT IN (
       '100169349' -- Hertzenberg, David (other div rep); 14
      ,'100000322' -- Terzakis, Eleni (other div rep); 82
      ,'100074349' -- Wynne, John (prior rep); 50
      ,'100000132' -- Dearden, Thomas (prior rep); 0
      ,'100010349' -- Hebbar, Vinay (CL and HE rep; Keylogic acct on cash basis); 96
      )
    and prd.product_number NOT IN (
       '3002S9' -- HBS Exec Ed Custom Program; 4
      ,'HBSXED' -- HBS Exec Ed Custom Program; 10
      ,'HBX' -- future product # exclusion added 11/3/2016
      ) 
 group by 
  -- cc.segment1
  --,cc.segment1_desc
  --,cc.segment2
  --,cc.segment2_desc
   prd.product_number
  ,prd.format
  ,prd.language
  ,prd.title_abbreviation
  ,pd.bill_to_party_id
  ,pa.party_name
  ,pa.party_number
  ,cb.cust_account_id
  ,cb.account_name
  ,cb.account_number
  ,cb.parent_number
  ,cb.parent_name
  ,cs.cust_account_id
  ,cs.account_name
  ,cs.account_number
  ,dt.fiscal_yyyymm
  ,dt.date_field
  ,pd.order_header_id
  ,plr.RESOURCE_NAME
 order by
  dt.date_field desc