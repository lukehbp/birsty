from numbers import Number

def proj_stagemap(projstage):
    if projstage == 'Scheduled':
        finalmap = 'Schl'
    elif projstage == 'Unscheduled':
        finalmap = 'UnSchl'
    elif projstage == 'Soft Scheduled':
        finalmap = 'SoftSchl'
    elif projstage == 'Project Initiated':
        finalmap = 'ProjInit'
    elif projstage == 'Closed':
        finalmap = 'Closed'
    elif projstage == 'Canceled':
        finalmap = 'Canceled'
    else:
        finalmap = 'NA'
    return finalmap


def revmapping(cols):
    projstage = cols['Proj_Stage']
    revid = cols['RevID']
    if revid == 1:
        finalmap = 'RecRev'
    elif revid == '1':
        finalmap = 'RecRev'
    elif revid == 2:
        finalmap = 'DefRev'
    elif revid == 3:
        finalmap = 'BillEv'
    elif revid == 'Opportunity Schedule':
        finalmap = 'PipeLn'
    elif revid == 'Manual - SD':
        finalmap = projstage
    elif revid == 'PO Hold':
        finalmap = 'POH'
    else:
        finalmap = 'unmapped'
    return finalmap


def wfc(cols):
    amount = cols['Amount']
    revid = cols['RevID']
    prob = cols['Opp_Prob']
    if revid == 'PipeLn':
        wfcA = (prob/100) * amount
    else:
        wfcA = amount
    return wfcA


def birstacctmap(IDs):
    if type(IDs['OppsBirst_Account']) == type('string'):
        finalID = IDs['OppsBirst_Account']
    elif type(IDs['Acct_Id']) == type('string'):
        finalID = IDs['Acct_Id']
    elif (type(IDs['Prnt_Id']) == type('string')):
        finalID = IDs['Prnt_Id']
    else:
        finalID = 'unmapped'
    return finalID


def findsrc(IDs):
    if type(IDs['OppsBirst_Account']) == type('string'):
        finalID = 'opp'
    elif type(IDs['Acct_Id']) == type('string'):
        finalID = 'bill2'
    elif (type(IDs['Prnt_Id']) == type('string')):
        finalID = 'prnt'
    else:
        finalID = 'unmapped'
    return finalID

def fp(caldate):
    # caldate = datetime.strptime(caldate_str, '%Y-%m-%d')
    mnth = caldate.month
    yr = caldate.year
    if mnth >= 7:
        rtFP = ((yr+1) * 100)+(mnth-6)
    else:
        rtFP = ((yr) * 100) + (mnth + 6)
    return rtFP


def fc_filter(cols):
    # ['revFY', 'RevID']
    revfyfp = cols['revFYFP']
    revid = cols['RevID']
    oppprob = cols['Opp_Prob']
    oppnum = cols['Opp_Num']
    if (revfyfp <= 201712) & (revid == 'RecRev'):
        fc_flag = 'Yes'
    elif (revid == 'PipeLn') & (oppprob > 0):
        fc_flag = 'Yes'
    elif (revfyfp >= 201801) & (revid in ['RecRev','BillEv', 'DefRev', 'Schl']):
        fc_flag = 'Yes'
    else:
        fc_flag = 'No'
    return fc_flag

def revfy(fp):
    fp_str = str(fp)
    fy = 'FY' + fp_str[2:4]
    return fy


def tcvfyfp(cols):
    tcv_src = cols['src']
    trxfyfp = cols['trxFYFP']
    oppfyfp = cols['Opp_CloseMonth']
    if tcv_src == 'actual':
        fyfp = trxfyfp
    elif tcv_src == 'unw_pl':
        fyfp = oppfyfp
    elif tcv_src == 'sf_non_tcv':
        fyfp = oppfyfp
    else:
        fyfp = 'other'
    return fyfp

def fctcv(cols):
    tcv_src = cols['src']
    sum1 = cols['Amount']
    sum2 = cols['TCV_UnWPL']
    if tcv_src == 'actual':
        fc_tcv = sum1
    elif tcv_src == 'unw_pl':
        fc_tcv = sum2
    elif tcv_src == 'sf_non_tcv':
        fc_tcv = 0
    else:
        fc_tcv = 'other'
    return fc_tcv

def wtcv(cols):
    amount = cols['unw_fc_tcv']
    tcv_src = cols['src']
    prob = cols['Opp_Prob']
    if tcv_src == 'unw_pl':
        wfcC = (prob/100) * amount
    elif tcv_src == 'sf_non_tcv':
        wfcC = 0
    elif tcv_src == 'actual':
        wfcC = amount
    else:
        wfcC = 'other'
    return wfcC

def wtcv_pl(cols):
    amount = cols['unw_fc_tcv']
    tcv_src = cols['src']
    prob = cols['Opp_Prob']
    if tcv_src == 'unw_pl':
        wfcC = (prob/100) * amount
    else:
        wfcC = 0
    return wfcC

def wtcv_OppAmount_fc(cols):
    amount = cols['Opps_UnWAmount']
    revid = cols['RevID']
    prob = cols['Opp_Prob']
    if revid == 'PipeLn':
        wfcA = (prob/100) * amount
    else:
        wfcA = amount
    return wfcA

def wtcv_OppAmount_tcv(cols):
    amount = cols['Opps_UnWAmount']
    tcv_src = cols['src']
    prob = cols['Opp_Prob']
    if tcv_src == 'unw_pl':
        wfcC = (prob/100) * amount
    else:
        wfcC = amount
    return wfcC

def birst_owner(cols):
    revid = cols['RevID']
    oppowner = cols['OppOwner']
    acctowner = cols['AcctOwner']
    if revid == 'PipeLn':
        birstowner = oppowner
    else:
        birstowner = acctowner
    return birstowner


def birst_owner_tcv(cols):
    revid = cols['src']
    oppowner = cols['OppOwner']
    acctowner = cols['AcctOwner']
    if revid == 'unw_pl':
        birstowner = oppowner
    else:
        birstowner = acctowner
    return birstowner


def mile_opp(cols):
    mile_opp = cols['MileOppLine_OppNum']
    opp_opp = cols['Opp_Num']
    if type(mile_opp) == type('string'):
        fin_opp = mile_opp
    else:
        fin_opp = opp_opp
    return fin_opp


def mile_close(cols):
    mile_opp = cols['MileOppLine_MonthClose']
    opp_opp = cols['Opp_CloseMonth']
    if type(mile_opp) == type('string'):
        fin_opp = mile_opp
    else:
        fin_opp = opp_opp
    return fin_opp

def mile_prob(cols):
    mile_opp = cols['MileOppLine_OppProb']
    opp_opp = cols['Opp_Prob']
    mile_opp_num = cols['MileOppLine_OppNum']
    if type(mile_opp_num) == type('string'):
        fin_opp = mile_opp
    else:
        fin_opp = opp_opp
    return fin_opp

def mile_stage(cols):
    mile_opp = cols['MileOppLine_OppStage']
    opp_opp = cols['Opp_Stage']
    if type(mile_opp) == type('string'):
        fin_opp = mile_opp
    else:
        fin_opp = opp_opp
    return fin_opp

def mile_type(cols):
    mile_opp = cols['MileOppLine_OppType']
    opp_opp = cols['Opp_Type']
    if type(mile_opp) == type('string'):
        fin_opp = mile_opp
    else:
        fin_opp = opp_opp
    return fin_opp

def mile_amount(cols):
    mile_opp = cols['MileOppLine_OppAmount']
    opp_opp = cols['Opps_Amount']
    mile_opp_num = cols['MileOppLine_OppNum']
    if type(mile_opp_num) == type('string'):
        fin_opp = mile_opp
    else:
        fin_opp = opp_opp
    return fin_opp

def mile_type2(cols):
    mile_type_id = cols['Mile_Type']
    if mile_type_id == '012E0000000fBNOIA2':
        mile_type_name = 'Delivery'
    elif mile_type_id == '012E0000000fBNPIA2':
        mile_type_name = 'Revenue'
    elif mile_type_id == '0120L000000NQCGQA4':
        mile_type_name = 'Task'
    else:
        mile_type_name = mile_type_id
    return mile_type_name

def mile_order(cols):
    mile_opp = cols['Order_Name']
    opp_opp = cols['Trx_OrderNum']
    if type(mile_opp) == type('string'):
        fin_opp = mile_opp
    else:
        fin_opp = opp_opp
    return fin_opp