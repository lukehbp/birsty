from sfpull import sfpullfunc_flat as sfflat
from sfpull import sfpullfunc as sf
from mapfuncs import mapfuncsfuncs as mf
from obiee import obiee_conn
from obiee import obiee_conn_flat as obiee_flat
import json
import pandas as pd
import time

# date var used for saving file names
date1 = time.strftime('_%Y_%m_%d')
date2 = time.strftime('%Y_%m_%d_')
# get obiee and sfdc sources

#sfresults = sf.sffunc()
sfresults = sfflat.sffunc_flat()
obiee = obiee_conn.getobiee()
obiee = obiee_flat.getobiee_flat()



# obiee = obiee_flat.getobiee_flat()


# get consolidated system sources and set to local variable
tcvfact = obiee['tcv']
revfact = obiee['revfact']
sfofact = sfresults['Fact_Trx']
sfofact_tcv = sfresults['Fact_Tcvpl']
# *************************************************************

# supplement obiee facts withf# salesforce info
# join json has flag to apply function for determining final account id
# 1 look up is from obiee - order to opp number mapping
with open('birsty_joins.json') as json_data:
    birsty_joins = json.load(json_data)
for jn in birsty_joins['joins']:
    if jn['src'] == 'func':
        dtypeCount = [revfact.iloc[:, i].apply(type).value_counts() for i in range(revfact.shape[1])]
        print(dtypeCount)
        revfact['acct_src'] = revfact[['OppsBirst_Account', 'Acct_Id', 'Prnt_Id']].apply(mf.findsrc, axis=1)
        revfact['Final_AcctID'] = revfact[['OppsBirst_Account', 'Acct_Id', 'Prnt_Id']].apply(mf.birstacctmap, axis=1)
        tcvfact['Final_AcctID'] = tcvfact[['OppsBirst_Account', 'Acct_Id', 'Prnt_Id']].apply(mf.birstacctmap, axis=1)
    elif jn['src'] == 'sf':
        revfact = pd.merge(revfact, sfresults[jn['right']], left_on=jn['left_on'], right_on=jn['right_on'], how='left', suffixes=(jn['suf1'], jn['suf2']))
        tcvfact = pd.merge(tcvfact, sfresults[jn['right']], left_on=jn['left_on'], right_on=jn['right_on'], how='left', suffixes=(jn['suf1'], jn['suf2']))
    elif jn['src'] == 'obiee':
        revfact = pd.merge(revfact, obiee[jn['right']], left_on=jn['left_on'], right_on=jn['right_on'], how='left', suffixes=(jn['suf1'], jn['suf2']))
        tcvfact = pd.merge(tcvfact, obiee[jn['right']], left_on=jn['left_on'], right_on=jn['right_on'], how='left', suffixes=(jn['suf1'], jn['suf2']))
    # revfact.to_csv(jn['right'] + '.csv')

dirpath = 'dataflow/2fullfacts/'
tcvfact.to_csv(dirpath + 'fullobiee_tcvfacts.csv', index=False, header=True, index_label=False)
revfact.to_csv(dirpath + 'fullobiee_revfacts.csv', index=False, header=True, index_label=False)
sfofact.to_csv(dirpath + 'fullsfdc_revfacts.csv', index=False, header=True, index_label=False)
sfofact_tcv.to_csv(dirpath + 'fullsfdc_tcvfacts.csv', index=False, header=True, index_label=False)
# tcvfact.to_csv(dirpath + '/hist/' + date2 + 'fullobiee_tcvfacts.csv', index=False, header=True, index_label=False)
# revfact.to_csv(dirpath + '/hist/' + date2 + 'fullobiee_revfacts.csv', index=False, header=True, index_label=False)
# sfofact.to_csv(dirpath + '/hist/' + date2 + 'fullsfdc_revfacts.csv', index=False, header=True, index_label=False)

# *************************************************************
# combine obiee and sfdc sources (first for rev, then tcv)
# normalize column names
with open('obieefact_colnames.json') as json_data:
    cols = json.load(json_data)
# merge obiee and sf rev data
revfact = revfact.filter(cols.keys())
revfact = revfact.rename(columns=cols)
sfofact = sfofact.filter(cols.keys())
sfofact = sfofact.rename(columns=cols)

# merge obiee and sfdc, write to csv
fcrev = pd.DataFrame(pd.concat([revfact, sfofact]))
# final mapping transforms
fcrev['Proj_Stage'] = fcrev['Proj_Stage'].apply(mf.proj_stagemap)
fcrev['RevID'] = fcrev[['RevID', 'Proj_Stage']].apply(mf.revmapping, axis=1)
fcrev['wfc'] = fcrev[['Amount', 'RevID', 'Opp_Prob']].apply(mf.wfc, axis=1)
fcrev['Opps_WAmount'] = fcrev[['Opps_UnWAmount', 'RevID', 'Opp_Prob']].apply(mf.wtcv_OppAmount_fc, axis=1)
fcrev['unwfc'] = fcrev['Amount']
fcrev['revFY'] = fcrev['revFYFP'].apply(mf.revfy)
fcrev['fc_filter'] = fcrev[['revFYFP', 'RevID', 'Opp_Prob','Opp_Num']].apply(mf.fc_filter, axis=1)
# change fc_filer to first column
colnames = fcrev.columns.tolist()
colnames = colnames[-1:] + colnames[:-1]
fcrev = fcrev[colnames]
# birst_owner
# fcrev = fcrev.loc[~fcrev['Account'].isin['HD Test', 'CL Training & Test', 'Astadia Test Account']]
fcrev = fcrev[-fcrev['Account'].isin(['HD Test', 'CL Training & Test', 'Astadia Test Account'])]
fcrev = fcrev[-fcrev['revFY'].isin(['FY12','FY13'])]
fcrev = fcrev.drop(['Amount'], axis=1)
fcrev['BirstOwner'] = fcrev[['RevID', 'AcctOwner', 'OppOwner']].apply(mf.birst_owner, axis=1)

# merge obiee and sf tcv data
sfofact_tcv = sfofact_tcv.filter(cols.keys())
sfofact_tcv = sfofact_tcv.rename(columns=cols)
tcvfact = tcvfact.filter(cols.keys())
tcvfact = tcvfact.rename(columns=cols)

tcvfact['src'] = 'actual'
sfofact_tcv['src'] = 'unw_pl'

fctcv = pd.DataFrame(pd.concat([tcvfact, sfofact_tcv]))
# add in sf opps not reported in pipeline or actuals
opps_ref = sfresults['Ref_Opps']
reported_opps = fctcv['Opp_Num'].drop_duplicates()
sf_non_tcv = opps_ref[-opps_ref['Opps_Opportunity__c'].isin(reported_opps)]
sf_non_tcv = sf_non_tcv.drop(['Opps_Id'], axis=1)
sf_non_tcv = sf_non_tcv.rename(index=str, columns={"Opps_Opportunity__c": "Opp_Num", "Opps_Month_Close__c": "Opp_CloseMonth"})
sf_non_tcv = sf_non_tcv.rename(index=str, columns={"Opps_Probability": "Opp_Prob", "Opps_StageName": "Opp_Stage"})
sf_non_tcv = sf_non_tcv.rename(index=str, columns={"Opps_Type": "Opp_Type", "Opps_Amount": "unw_non_tcv"})
sf_non_tcv['src'] = 'sf_non_tcv'
fctcv = pd.DataFrame(pd.concat([fctcv, sf_non_tcv]))
# fctcv.to_csv('preTranTCV.csv')
# final mapping transforms
fctcv = fctcv.rename(index=str, columns={"revFYFP": "trxFYFP"})
fctcv['tcvFYFP'] = fctcv[['Opp_CloseMonth', 'trxFYFP', 'src']].apply(mf.tcvfyfp, axis=1)
fctcv['tcvFY'] = fctcv['tcvFYFP'].apply(mf.revfy)
fctcv['unw_fc_tcv'] = fctcv[['Amount', 'TCV_UnWPL', 'src']].apply(mf.fctcv, axis=1)
fctcv['w_fc_tcv'] = fctcv[['unw_fc_tcv', 'Opp_Prob', 'src']].apply(mf.wtcv, axis=1)
fctcv = fctcv[-fctcv['Account'].isin(['HD Test', 'CL Training & Test', 'Astadia Test Account'])]
fctcv = fctcv[-fctcv['tcvFY'].isin(['FY12','FY13'])]

fctcv['w_pl_tcv'] = fctcv[['unw_fc_tcv', 'Opp_Prob', 'src']].apply(mf.wtcv_pl, axis=1)
fctcv = fctcv.rename(index=str, columns={'TCV_UnWPL': 'unw_pl_tcv', 'Amount': 'actual_tcv'})
fctcv['Opps_WAmount'] = fctcv[['Opps_UnWAmount', 'Opp_Prob', 'src']].apply(mf.wtcv_OppAmount_tcv, axis=1)
fctcv['BirstOwner'] = fctcv[['src', 'AcctOwner', 'OppOwner']].apply(mf.birst_owner_tcv, axis=1)

# fctcv = fctcv.drop(['TCV_UnWPL'], axis=1)
# fctcv = fctcv.drop(['Amount'], axis=1)

# write file
dirpath = 'dataflow/3fc/'
boxpath = 'C:/Users/luke.grymek/Box Sync/CL Finance & Operations/2. Analytics/cl datasheet/datasheet 2.0/'
fctcv.to_csv(dirpath + 'tcv' + date1 + '.csv', index = False)
# fctcv.to_csv(boxpath + 'tcv' + date1 + '.csv', index = False)
fcrev.to_csv(dirpath + 'fc' + date1 + '.csv', index = False)
# fcrev.to_csv(boxpath + 'fc' + date1 + '.csv', index = False)
# *************************************************************
# longterm - add in new hmm-le terms