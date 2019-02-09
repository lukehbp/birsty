from simple_salesforce import Salesforce
import json
import pandas as pd
from mapfuncs import mapfuncsfuncs as mf
import time

def sffunc():
    # date var used for saving file names
    date1 = time.strftime('%Y_%m_%d_')

    # get connection info and connect to sf api
    with open('sfpull/soql/a00_conn.json') as json_data:
        conn = json.load(json_data)['conns'][0]
    sf = Salesforce(username=conn['u1n'], password=conn['p1w'], security_token=conn['st'])
    # ****************************************

    # get json workflow meta data
    with open('sfpull/soql/a0_jsons.json') as json_data:
        json_files_descs = json.load(json_data)

    # iterate through meta data to load in each json
    json_wf = {}
    for json_file_desc in json_files_descs['jsons']:
        with open(json_file_desc['file']) as json_data:
            json_file = json.load(json_data)
        json_wf[json_file_desc['name']] = json_file
    # ****************************************

    # extract from meta data workflow sf api queries
    # steps 1-5
    queries = json_wf['queries']
    sf_q_results = {}
    for q in queries['queries']:
        # 1 get the actual query
        fd = open(q['query'], 'r')
        sf_q = fd.read()
        fd.close()
        # 2 query the api
        sf_q_result = sf.query_all(sf_q)
        # 3 unpack the query result json
        sf_q_result = pd.DataFrame(sf_q_result['records'])
        dirpath = 'dataflow/0fullpulls'
        sf_q_result.to_csv(dirpath + '/sffull_' + q['name'] + '.csv', index=False)
        # sf_q_result.to_csv(dirpath + '/hist/' + date1 + 'sffull_' + q['name']+ '.csv', index=False)
        sf_q_result = sf_q_result.drop(['attributes'], axis=1)
        # 4 add a prefeix to all the column names to help track sourcing
        sf_q_result = sf_q_result.add_prefix(q['Prefix'])
        # 5 final output for sf api query to list of dataframes (potential to write to csv)
        sf_q_results[q['name']] = sf_q_result
        # #sf_q_result.to_csv('sfdc_raw_' + q['name'] + '.csv', index=False, header=True, index_label=False)
    # ****************************************

    # join data to combine reference info and fact table for sch bl + pipeline
    joins = json_wf['joins']
    sf_rev = sf_q_results['Fact_Trx']
    for join in joins['joins']:
        rj = sf_q_results[join['right']]
        lon = join['left_on']
        ron = join['right_on']
        # work around when adding in opp owner field
        if lon == "Opps_OwnerId":
            sf_rev = pd.merge(sf_rev, rj, left_on=lon, right_on=ron, how='left', suffixes=('', '_Opps'))
        elif lon == "Oppline_OpportunityId":
            sf_rev = pd.merge(sf_rev, rj, left_on=lon, right_on=ron, how='left', suffixes=('', '_MileOpp'))
        else:
            sf_rev = pd.merge(sf_rev, rj, left_on=lon, right_on=ron, how='left', suffixes=('', '_Acct'))

    # joins are done, now finalizing datasheet
    # change col names
    sf_rev.to_csv('dataflow/1datasources/soql/prefincols.csv')
    fincols = json_wf['fincols']
    sf_rev = sf_rev.rename(columns=fincols)
    sf_rev = sf_rev.filter(fincols.values())
    # convert calendar dates to fiscal period

    sf_rev['Opp_Num'] = sf_rev[['MileOppLine_OppNum', 'Opp_Num']].apply(mf.mile_opp, axis=1)
    sf_rev['Opp_CloseMonth'] = sf_rev[['MileOppLine_MonthClose', 'Opp_CloseMonth']].apply(mf.mile_close, axis=1)
    sf_rev['Opp_Prob'] = sf_rev[['MileOppLine_OppProb', 'Opp_Prob','MileOppLine_OppNum']].apply(mf.mile_prob, axis=1)
    sf_rev['Opp_Stage'] = sf_rev[['MileOppLine_OppStage', 'Opp_Stage']].apply(mf.mile_stage, axis=1)

    sf_rev['Trx_OrderNum'] = sf_rev[['Order_Name', 'Trx_OrderNum']].apply(mf.mile_order, axis=1)

    sf_rev['Opp_Type'] = sf_rev[['MileOppLine_OppType', 'Opp_Type']].apply(mf.mile_type, axis=1)
    sf_rev['Opps_Amount'] = sf_rev[['MileOppLine_OppAmount', 'Opps_Amount','MileOppLine_OppNum']].apply(mf.mile_amount, axis=1)

    sf_rev['Mile_Type'] = sf_rev[['Mile_Type']].apply(mf.mile_type2, axis=1)

    sf_rev['Trx_Month'] = pd.to_datetime(sf_rev['Trx_Month'])
    sf_rev['Trx_Month'] = sf_rev['Trx_Month'].apply(mf.fp)
    sf_rev['Opp_CloseMonth'] = pd.to_datetime(sf_rev['Opp_CloseMonth'])
    sf_rev['Opp_CloseMonth'] = sf_rev['Opp_CloseMonth'].apply(mf.fp)
    sf_q_results['Fact_Trx'] = sf_rev
    # sf_rev.to_csv('sfFact.csv', index=False, header=True, index_label=False)
    # ****************************************
    # use sf fact df to calculate tcv pipeline

    sf_tcv = sf_rev[sf_rev['Trx_Type'].isin(['Opportunity Schedule'])]
    indx = ['Opp_Num', 'Trx_Offering']
    sf_tcv = sf_tcv.pivot_table(index=indx, values='Trx_Amount', aggfunc='sum')
    sf_tcv = sf_tcv[sf_tcv['Trx_Amount'] != 0]
    sf_tcv = sf_tcv.reset_index()

    ref_fields = ['Opp_Num', 'Opp_Stage', 'Opp_Type', 'Opp_CloseMonth', 'Opp_Prob', 'Opps_Amount', 'Acct_Name', 'Acct_Owner', 'Opp_Owner', 'Acct_RegionHunter']
    OppRef = sf_rev[ref_fields]
    OppRef = OppRef.drop_duplicates()

    sf_tcv = pd.merge(sf_tcv, OppRef, left_on='Opp_Num', right_on='Opp_Num', how='left')
    sf_tcv = sf_tcv.rename(index=str, columns={"Trx_Amount": "TCV_UnWPL"})

    sf_q_results['Fact_Tcvpl'] = sf_tcv
    # ****************************************

    # special adjustments 1-4:
    # 1 account reference
    # a - take out null party id's (party id sf - bill2party obiee)
    # b - change id variable type to prevent lookup error
    sfacct = sf_q_results['Ref_Acct']
    sfacct = sfacct[sfacct['Acct_Party_ID__c'].notnull()]
    sfacct['Acct_Party_ID__c'] = pd.to_numeric(sfacct['Acct_Party_ID__c']).astype(dtype='int64')
    sf_q_results['Ref_BillParty'] = sfacct
    # 2 parent reference - similar idea as account ref
    sfprnt = sf_q_results['Ref_Prnt']
    sfprnt = sfprnt[sfprnt['Prnt_Registry_ID__c'].notnull()]
    sf_q_results['Ref_Prnt'] = sfprnt
    # sfprnt.to_csv('sfdc_Ref_Prnt.csv')

    # 3 offering references
    # a - remove duplicate rows
    # b - sort and only take the first instance of each offering-prod mapping
    #     (there are a few instances of repeat mappings)
    ref_sfdcoff = sf_q_results['Ref_Offr']
    ref_sfdcoff = ref_sfdcoff[['Offr_Offering__c', 'Offr_Product_Number__c']]
    ref_sfdcoff = ref_sfdcoff.drop_duplicates()
    ref_sfdcoff = ref_sfdcoff[ref_sfdcoff['Offr_Product_Number__c'].notnull()]
    ref_sfdcoff = ref_sfdcoff.drop_duplicates(['Offr_Product_Number__c'])
    sf_q_results['Ref_Offr'] = ref_sfdcoff
    # ref_sfdcoff.to_csv('ref_sfdcofferingMOD.csv', index=False, header=True, index_label=False)

    # 4 change close date to close fp on opp reference
    ref_sfdcopp = sf_q_results['Ref_Opps']
    ref_sfdcopp['Opps_Month_Close__c'] = pd.to_datetime(ref_sfdcopp['Opps_Month_Close__c'])
    ref_sfdcopp['Opps_Month_Close__c'] = ref_sfdcopp['Opps_Month_Close__c'].apply(mf.fp)
    sf_q_results['Ref_Opps'] = ref_sfdcopp

    # *************************************************************
    # account-opp num mappings have nested json fields
    # this is a serpate workflow than the other api calls to unnest the fields
    # sf_q_results['Ref_Opps_Birst']
    # nested parent in the parent query was a pain.  this works so I am going with it
    sf_q_results['Ref_Opps_Birst'] = getopp(sf)

    # save results to csv
    dirpath = 'dataflow/1datasources/soql/'
    for sqr in sf_q_results:

        sf_q_results[sqr].to_csv(dirpath + sqr + '.csv', index=False, header=True, index_label=False)
        # sf_q_results[sqr].to_csv(dirpath + '/hist/' + date1 + sqr + '.csv', index=False, header=True, index_label=False)
    return sf_q_results


def getopp(sfconn):
    fd1 = open('sfpull/soql/queries/ref_sfdcopps_birst.sql', 'r')
    q_soql = fd1.read()
    fd1.close()
    records = sfconn.query_all(q_soql)
    oppnum = []
    acctid = []
    i = 0
    for record in records['records']:
        oppnum.append(record['Opportunity__c'])
        if record['Account'] is None:
            acctid.append('None')
        else:
            acctid.append(record['Account']['Id'])
        i = i + 1
    lb = ['OppsBirst_Opportunity__c', 'OppsBirst_Account']
    list_cols = [oppnum, acctid]
    data = dict(list(zip(lb, list_cols)))
    ref_opp = pd.DataFrame(data)
    return ref_opp
