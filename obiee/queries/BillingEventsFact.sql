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
  ,plr.RESOURCE_NAME as "ebs_rep"
  ,dt.fiscal_yyyymm
  ,dt.date_field "Transaction Date"
  ,3 "Revenue Type ID" -- Billing Events
  ,pd.order_header_id
  ,sum(pd.future_billing_amount) as "Amount"
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
    left outer join pipeline_order_lines pol
      on pd.order_line_id=pol.order_line_id
    left outer join pipeline_resources pr
      on pd.resource_id=pr.resource_id
    left outer join customers_bill_to cb
      on pd.bill_to_customer_id=cb.cust_account_id
    left outer join customers_sold_to cs
      on pd.sold_to_customer_id=cs.cust_account_id
	left join pipeline_resources plr
	  on pd.RESOURCE_ID = plr.RESOURCE_ID
  where dt.fiscal_yyyymm >= '201601' --'201703'
    --and rownum < 10 
    --and cc.segment1='01'
    --and cc.segment2='610'
    and ps.description IN ('Future Billing')
    and prd.product_number 
      NOT IN ( 'ULV11' -- Credit for Unused HMM Lic
              ,'ULV11A'-- HMM v11 Unused Lic. Credit
             )
    and pol.accounting_rule NOT IN ('BLP Ratable', 'DEFERRED-OPEN')
    and pr.resource_id<> '100169349' -- Hertzenberg, David
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