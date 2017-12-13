#! /opt/catalyst/insight/ve/veMNR_2711/bin/python
import sys
if sys.platform[:3] == 'lin':
    sys.path.append('/opt/catalyst/insight/wopr/app/modules')
    sys.path.append('/opt/catalyst/insight/reports/dsiReports')
    sys.path.append('/opt/catalyst/er/eric')
else:
    sys.path.append(r'c:\work\repos\mnr\trunk\wopr\app\modules')
    sys.path.append(r'c:\work\repos\mnr\trunk\reports\dsiReports')
    sys.path.append(r'c:\work\repos\mnr\trunk\eric')
import catalystTools.jam.dataService as ds
import catalystTools.jam.methods as meth
import catalystTools.auth as auth
import tools.reports as report
import tools.search as search
import tools.admin as admin
import runReports as runrep
import datetime as dat
import HTMLParser
import threading
import time
import os


HP = HTMLParser.HTMLParser()
APP = 'insight'
MNR = 'psdreporting'
USER = auth.getAppUser(MNR)
PASS = auth.getAppPassword(MNR)
SEARCHPARAMS = search.ADD_SEARCH_PARAMS
SEARCHPARAMS['catchAll'] = '{"type":"facetedSearch"}'
REPORTNAME = 'StandardPrivilegeLog'
OUTDIR = ''
MAX_THREADS = 5
PRIV_MASTER = [
                '1-Privileged', '395.0191 Credentialing', '395.0193 FS Peer Review',\
                'Attorney-Client', 'Attorney-Client Work Product', 'Financial',\
                'PHI', 'Privilege', 'Privileged', 'Trade Secret', 'Work Product',
                'Wholly Privileged', 'Partially Privileged', '1-Needs Redaction',\
                '2-Privilege AC', '3-Privilege QA', '4-Privilege WP', \
                '5-Potentially Privileged', '1-PotentiallPrivilege'
                ]
REVSTAT_MASTER = [
                'Privilege', 'Privileged', '6-ResponsivePrivilege',\
                '8-NotResponsivePrivilege', 'Privileged Non-Responsive',\
                'Privileged Redact Responsive', 'Privileged Responsive'
                ]
FIELDS = [
            'Author', 'BegAtt', 'BegControl', 'Cc', 'Comments', 'DocType',\
            'EndAtt', 'EndControl', 'FamilyDate', 'From', 'Issues', 'PageCount',\
            'Privileged', 'ProdBegAttach', 'ProdBegControl', 'ProdEndAttach',\
            'ProdEndControl', 'SentDate', 'Subject', 'To'
            ]



def get_lookup_data(casename, lookup):
    lkpval = admin.get_lookup_values(casename, lookup, user=USER, password=PASS)
    lookupval = lkpval.get('lookups').get('lookup', {}).get('entry', [])

    if type(lookupval) is dict:
        lookupval = [lookupval]

    return {lookup : [val.get('entry') for val in lookupval]}


def get_case_lookup_values(caselist, field):
    lookup_values = {}
    for casename in caselist:
        lookup = get_lookup_data(casename, field)

        val_list = []
        for value in lookup[field]:
            if field.lower() == 'privileged':
                if value in PRIV_MASTER:
                    val_list.append(value)
            else:
                if value in REVSTAT_MASTER:
                    val_list.append(value)

        lookup_values[casename] = {field : val_list}

    return lookup_values


def build_privilege_searches(casename, priv_vals, revstat_vals):
    # get length of both privileged and review status
    priv_values = priv_vals.get(casename).get('Privileged', [])
    revstat_values = revstat_vals.get(casename).get('ReviewStatus', [])

    # if both are 0 return none
    if len(priv_values) == 0 and len(revstat_values) == 0:
        return None

    priv_value = '" "'.join(priv_values)
    priv_string = 'Privileged = ["%s"]' % priv_value

    if len(revstat_values) == 0:
        # if the reviewstatus field does not have privileged designation, only use privileged
        searchstring = priv_string
    else:
        # otherwise use both
        revstat_value = '" "'.join(revstat_values)
        revstat_string = 'ReviewStatus = ["%s"]' % revstat_value
        searchstring = '((%s) OR (%s))' % (priv_string, revstat_string)

    return searchstring


def get_search_counts(casename, searchstring, collections):
    searchcount = 0
    counts = search.get_count_json(casename, searchstring, collections, USER, PASS)

    if counts.get('messageId', '0') == '200':
        searchcount = int(counts.get('count'))

    return searchcount


def get_searchdata(params, displayname, txt_file):
    # set the lock for this thread
    lock = threading.Lock()
    lock.acquire()
    result = meth.GetSearchDataById(params, 'dict').body

    try:
        result = result.get('documents', {}).get('document', [])
    except:
        time.sleep(5)
        params['truncate'] = 'true'
        result = meth.GetSearchDataById(params, 'dict').body

    # release the lock
    lock.release()

    if result == {} or result is None:
        return []

    if type(result) is dict:
        result = [result]

    # create a string to hold each line of data
    data = ''

    # loop through each line of data and save to string
    for priv in result:
        data += '"%s","%s"' % (displayname, priv.get('orba_uri', '')) # docid

        for field in FIELDS:
            fieldval = priv.get(field)

            if fieldval is None:
                data += ',""'
            elif type(fieldval) is list:
                fieldval = HP.unescape('; '.join(fieldval))
                if len(fieldval) > 4000:
                    fieldval = fieldval[:4000]

                fieldval = fieldval.replace('"', '""')
                data += ',"%s"' % fieldval
            else:
                fieldval = HP.unescape(fieldval)
                if len(fieldval) > 4000:
                    fieldval = fieldval[:4000]

                fieldval = fieldval.replace('"', '""')
                data += ',"%s"' % fieldval

        data += '\n'

    # write the string data to the text file
    txt_open = open(txt_file, 'wb')
    txt_open.write(data.encode('utf-8'))
    txt_open.close()
    os.chmod(txt_file, 0777)

    return txt_file


def get_threaded_data(casename, displayname, searchcount):
    # determine page sizes and number of pages
    page_size = 500

    if searchcount % page_size == 0:
        num_pages = searchcount / page_size
    else:
        num_pages = searchcount / page_size + 1

    # get searchid
    searchid = search.addsearch(casename, SEARCHPARAMS, USER, PASS)
    if searchid is None or searchid == '':
        print 'searchid failed: %s' % casename

    params = {}
    params['caseName'] = casename
    params['searchid'] = searchid
    params['fieldlist'] = ','.join(FIELDS)
    params['pagesize'] = page_size
    params['getScoreAndConfidence'] = 'false'
    params['truncate'] = 'false'
    params['getSnippets'] = 'false'
    params['offSet'] = ''
    params['getTotalCount'] = 'true'

    # loop through the pages and get the privilege data
    txt_filelist = []
    threadlist = []
    for page_num in range(1, num_pages + 1):
        while len(threadlist) >= MAX_THREADS:
            threadlist = [t for t in threadlist if t.isAlive()]
            time.sleep(0.05)
            continue

        # set the file/thread name
        txt_name = '%s_%s' % (casename, str(page_num).zfill(len(str(num_pages))))
        if sys.platform[:3] == 'lin':
            thread_dir = '/opt/catalyst/insight/reports/dsiReports/privilege_log_report'
        else:
            thread_dir = r'\\fs05\data\_psd\reports\dsi_reports\privilege_log_report'

        if not os.path.exists(thread_dir):
            os.makedirs(thread_dir, 0777)

        txt_file = os.path.join(thread_dir, '%s.txt' % txt_name)
        txt_filelist.append(txt_file)

        # add authkey and page number to the parameters
        params['currpage'] = page_num

        t = threading.Thread(target=get_searchdata, args=(params, displayname, txt_file), name=txt_name)
        t.daemon = True
        threadlist.append(t)
        t.start()

    # wait for all threads to complete
    while len(threadlist) > 0:
        threadlist = [t for t in threadlist if t.isAlive()]
        time.sleep(0.05)
        continue

    return txt_filelist


def save_privilege_data(casename, displayname, searchcount, csv_file):
    # run threads to get the data
    txt_filelist = get_threaded_data(casename, displayname, searchcount)

    # if no text file data was saved, log no data in csv
    if txt_filelist == []:
        return

    # loop through the list of text files and save to the csv file
    for txt_file in txt_filelist:
        txt_open = open(txt_file, 'rb')
        csv_open = open(csv_file, 'ab')

        for line in txt_open:
            csv_open.write(line)

        txt_open.close()
        csv_open.close()
        os.chmod(csv_file, 0777)

        # remove the text file to save space
        os.remove(txt_file)


def run(dsi_cases=None):
    # get run date
    rundate = str(dat.datetime.today()).split(' ')[0].replace('-', '')

    # bring in dsi case data
    if dsi_cases is None:
        dsi_cases = runrep.getPartnerCases('DSi')

    caselist = sorted(dsi_cases.keys())

    # set output dir and file locations
    if sys.platform[:3] == 'lin':
        OUTDIR = '/opt/catalyst/insight/reports/dsiReports'
    else:
        OUTDIR = report.get_output_dir('dsi_reports')

    OUTDIR = os.path.join(OUTDIR, 'privilege_log_report')
    if not os.path.exists(OUTDIR):
        os.makedirs(OUTDIR, 0777)

    csv_filename = '%s_%s.csv' % (REPORTNAME, rundate)
    csv_file = os.path.join(OUTDIR, csv_filename)

    # remove existing file if one exists
    if os.path.exists(csv_file):
        os.remove(csv_file)

    # save the csv file headers
    csv_open = open(csv_file, 'ab')
    csv_open.write('Project Name,Docid,%s\n' % ','.join(FIELDS))
    csv_open.close()
    os.chmod(csv_file, 0777)

    # get the privileged and review status values for all cases
    priv_vals = get_case_lookup_values(caselist, 'Privileged')
    revstat_vals = get_case_lookup_values(caselist, 'ReviewStatus')

    for casename in caselist:
        # get subcollections
        collections = ','.join(dsi_cases[casename].get('collections', ''))
        SEARCHPARAMS['subCollection'] = collections
        displayname = dsi_cases[casename].get('displayname')

        # build privilege and review status searches
        searchstring = build_privilege_searches(casename, priv_vals, revstat_vals)

        # if there are no priv or review status values write to csv and move on
        if searchstring is None:
            continue

        # add search string to globals
        SEARCHPARAMS['searchString'] = searchstring

        # get search count
        searchcount = get_search_counts(casename, searchstring, collections)

        # if count is 0 write to csv and move on
        if searchcount == 0:
            continue

        # get the case privilege data and save to csv file
        save_privilege_data(casename, displayname, searchcount, csv_file)

    # copy files to ftp location
    if sys.platform[:3] == 'lin':
        runrep.uploadReport(csv_file, '%s/%s' % (REPORTNAME, csv_filename))


if __name__ == '__main__':
    bt = time.clock()

    ds.authenticate(USER, PASS)

    run()

    et = time.clock()
    print round(et - bt, 2)
