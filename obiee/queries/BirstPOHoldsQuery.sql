SELECT
      hca.account_number bill_to_party,
      hca.account_name,
      oeh.order_number,
	  PO_HOLD.HOLD_UNTIL_DATE,
	  pb.date_field BILLING_EVENT_DATE,
	  DECODE (pol.po_hold, 'Y', 'PO Hold') forecast_category,
      oel.ordered_item product,
      SUM (pd.future_billing_amount) net_amount,
	  sp.salesrep_id
FROM
      PIPELINE_ORDER_LINES pol,
      PIPELINE_DETAIL pd,
	  PIPELINE_DATES pb,
      apps.mtl_system_items_b@ebs_apps_ro msi,
      ont.oe_order_lines_all@ebs_apps_ro oel,
      ont.oe_order_headers_all@ebs_apps_ro oeh,
      apps.hz_cust_accounts@ebs_apps_ro hca,
      apps.hz_cust_site_uses_all@ebs_apps_ro csua,
      apps.hz_cust_acct_sites_all@ebs_apps_ro csa,
      apps.ra_salesreps_all@ebs_apps_ro sp,
      ar.ra_rules@ebs_apps_ro rar,
	                (
                SELECT
                  po_oh.line_id, po_hs.hold_until_date
                FROM
                  apps.OE_HOLD_SOURCES_ALL@ebs_apps_ro po_hs,
                  apps.OE_ORDER_HOLDS_ALL@ebs_apps_ro po_oh,
                  apps.OE_HOLD_RELEASES@ebs_apps_ro po_hr,
                  apps.OE_HOLD_DEFINITIONS@ebs_apps_ro po_hd
                WHERE
                  1=1
                AND po_oh.hold_source_id  = po_hs.hold_source_id
                AND po_hs.hold_id         = po_hd.hold_id
                AND po_oh.HOLD_RELEASE_ID = po_hr.HOLD_RELEASE_ID (+)
                AND po_hd.name            = 'Multi-Year Contract Hold'
              ) PO_HOLD
WHERE 1=1
AND pol.order_line_id      = oel.line_id
AND oel.header_id          = oeh.header_id
AND oeh.salesrep_id        = sp.salesrep_id
AND oel.accounting_rule_id = rar.rule_id (+)
AND pol.ORDER_LINE_ID      = pd.ORDER_LINE_ID
AND pd.ORDER_HEADER_ID = oeh.header_id
AND csua.site_use_id       = oeh.invoice_to_org_id
AND csa.cust_acct_site_id  = csua.cust_acct_site_id
AND csa.cust_account_id    = hca.cust_account_id
AND msi.organization_id    = 83
AND msi.inventory_item_id  = oel.inventory_item_id
AND PB.DATE_KEY = pd.TRANSACTION_DATE_KEY
AND PO_HOLD.LINE_ID = OEL.LINE_ID
GROUP BY
	  hca.account_number,
      hca.account_name,
      oeh.order_number,
	  PO_HOLD.HOLD_UNTIL_DATE,
	  PB.DATE_FIELD,
	  DECODE (pol.po_hold, 'Y', 'PO Hold') ,
      oel.ordered_item  ,
	  sp.salesrep_id
HAVING 
      DECODE (pol.po_hold, 'Y', 'PO Hold') = 'PO Hold'
      AND SUM (pd.future_billing_amount) != 0
ORDER BY
      oeh.order_number