#!/opt/catalyst/insight/ve/veMNR_2711/bin/python
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
import tools.internal as internal
import catalystTools.auth as auth
import tools.reports as report
import tools.search as search
import tools.admin as admin
import runReports as runrep
import datetime as dat
import threading
import time
import os


APP = 'insight'
MNR = 'psdreporting'
USER = auth.getAppUser(MNR)
PASS = auth.getAppPassword(MNR)
ds.authenticate(USER, PASS)
SEARCHPARAMS = search.ADD_SEARCH_PARAMS
SEARCHPARAMS['catchAll'] = '{"type":"facetedSearch"}'
FIELDS = ['ReviewStatus', 'Privileged', 'Produced', 'RedactionStatus', 'Issues']
AMEC_FIELDS = ['PredictInfo', 'Confidential', 'HotDoc', 'QCStatus', 'RedactionReviewComplete', 'PrivilegeType']
AMEC_NOFACET = {'RedactionNeeded' : 'redactionreason', 'TechnicalIssueType' : 'techissue'}
RANG_FIELDS = ['Confidential', 'HotDoc', 'QCStatus', 'PredictInfo', 'PrivilegeType']
RANG_NOFACET = {'TechnicalIssueType' : 'techissue'}
AFFF_FIELDS = ['PredictInfo', 'RedactionReviewComplete']
AFFF_NOFACET = {'COC_OverviewReview' : 'cocoverviewreview'}
REPORTNAME = 'ReviewMetrics'
MAX_THREADS = 5
OUTDIR = ''


def get_custodianlist(casename):
    # get all custodians
    casecustodians = internal.get_facetdata(casename, 'custodian', '', USER, PASS)

    # normalize the name to all caps and dedupe
    custodians = {}
    for facet in casecustodians:
        custodian = facet.get('facet').upper().strip()
        custodian = custodian.replace(', ', '_').replace(',', '_').replace(' ', '_')
        custodian = custodian.lstrip(' \t\n\r').rstrip(' \t\n\r')
        count = int(facet.get('count'))

        if custodian not in custodians.keys():
            custodians[custodian] = count

        else:
            custodians[custodian] += count

    return custodians


def threaded_facets(custodian, casename, txt_file):
    # get searchid
    global SEARCHPARAMS

    #Lock the thread to use globals so the threads don't step on eachothers toes
    lock = threading.RLock()
    lock.acquire()
    nofacet_fields = None
    if casename == 'insdsijcio0002amecfoster':
        fieldlist = FIELDS + AMEC_FIELDS
        nofacet_fields = AMEC_NOFACET

    elif casename == 'insrang0001rangeresources':
        fieldlist = FIELDS + RANG_FIELDS
        nofacet_fields = RANG_NOFACET

    elif casename == 'insdsijcio0001affflitigation':
        fieldlist = FIELDS + AFFF_FIELDS
        nofacet_fields = AFFF_NOFACET

    else:
        fieldlist = FIELDS

    # get collections from search params
    collections = SEARCHPARAMS.get('subCollection')

    # set search string for custodian
    searchstring = '(custodian = "%s")' % custodian
    SEARCHPARAMS['searchString'] = searchstring
    searchid = search.addsearch(casename, SEARCHPARAMS, USER, PASS)
    lock.release()

    for field in fieldlist:
        params = {}
        params['caseName'] = casename
        params['facetname'] = field
        params['orderby'] = 'item-order'
        params['ordertype'] = 'ascending'
        params['limit'] = 10000
        params['searchid'] = searchid
        params['pattern'] = ''
        params['startFrom'] = ''

        # get all facet data
        for retry in range(0, 3): #Try harder loop
            try:
                response = meth.GetFacetDataForGlobalAdmin(params, 'dict')
                if response.status != 200:
                    raise Exception("GetFacetDataForGlobalAdmin failed with %s %s" % (response.status, response.reason))
                allfacetdata = response.body
                facet_list = allfacetdata.get('facet', {}).get('value', [])
                break
            except Exception as e:
                facet_list = []
                print "Retry %s for GetFacetDataForGlobalAdmin for field %s, case %s" % (retry, field, casename)
                print e
                time.sleep(2)

        if retry == 2 and facet_list == []:
            print "Failed to get facet data for field %s, case %s" % (field, casename)

        facetdata = ''
        if type(facet_list) is dict:
            facet_list = [facet_list]

        for facet in facet_list:
            value = facet.get('orbp_text', '')
            count = facet.get('orba_count', 0)

            if value == '':
                continue

            value = report.ascii_char_replace(value)
            custodian = custodian.replace('"', '""')
            field = field.replace('"', '""')
            value = value.replace('"', '""')
            count = count.replace('"', '""')
            facetdata += '"%s","%s","%s","%s"\n' % (custodian, field, value, count)

        # save to text file
        txt_open = open(txt_file, 'ab')
        txt_open.write(facetdata)
        txt_open.close()
        os.chmod(txt_file, 0777)

    # handle fields without a facet
    if nofacet_fields:
        facetdata = ''
        for field, lookup in nofacet_fields.iteritems():
            # get the lookup table values
            table_values = admin.get_lookup_values(casename, lookup, user=USER, password=PASS)
            table_values = table_values.get('lookups', {}).get('lookup', [])
            lookup_values = table_values.get('entry', None)

            if lookup_values is None:
                continue

            if type(lookup_values) is dict:
                lookup_values = [lookup_values]

            # loop through all lookup table values to run a search for the count
            for value_info in lookup_values:
                value = value_info.get('entry')
                valuesearch = '%s AND (%s = "%s")' % (searchstring, field, value)

                # get count of lookup value
                count_json = search.get_count_json(casename, valuesearch, collections, USER, PASS)
                count = count_json.get('count', '0')
                if count == '0':
                    continue

                # remove any ascii characters in value
                value = report.ascii_char_replace(value)

                # add data to string
                custodian = custodian.replace('"', '""')
                field = field.replace('"', '""')
                value = value.replace('"', '""')
                count = count.replace('"', '""')
                facetdata += '"%s","%s","%s","%s"\n' % (custodian, field, value, count)

        # save string to text file
        txt_open = open(txt_file, 'ab')
        txt_open.write(facetdata)
        txt_open.close()
        os.chmod(txt_file, 0777)


def get_custodiandata(allcustodians, casename, csv_file, displayname):
    # get domain data threaded by custodian
    threadlist = []
    txt_filelist = []
    for custodian, count in allcustodians.iteritems():
        # if threadlist is at MAX_THREADS, wait until one completes to move on
        while len(threadlist) >= MAX_THREADS:
            threadlist = [t for t in threadlist if t.isAlive()]
            time.sleep(0.05)
            continue

        # create a text file to hold domain data by custodian
        txt_filename = '%s_%s.txt' % (casename, custodian)
        if sys.platform[:3] == 'lin':
            thread_dir = '/opt/catalyst/insight/reports/dsiReports'

        else:
            thread_dir = r'\\fs05\data\_psd\reports\dsi_reports'

        thread_dir = os.path.join(thread_dir, 'review_metrics_report')
        if not os.path.exists(thread_dir):
            os.mkdir(thread_dir, 0777)

        txt_file = os.path.join(thread_dir, txt_filename)
        txt_filelist.append(txt_file)

        # get threaded domaindata
        t = threading.Thread(target=threaded_facets, args=(custodian, casename, txt_file), name=txt_filename)
        t.daemon = True
        threadlist.append(t)
        t.start()

    # wait for all threads to complete
    while len(threadlist) > 0:
        threadlist = [t for t in threadlist if t.isAlive()]
        time.sleep(0.05)
        continue

    # set string to hold the data
    displayname = displayname.replace('"', '""')
    custodiandata  = ''
    for txt_file in txt_filelist:
        if not os.path.exists(txt_file):
            print '%s - %s does not exist' % (casename, txt_file)
            continue

        # open the file and save to csv
        txt_open = open(txt_file, 'rb')
        for line in txt_open:
            custodiandata += '"%s",%s' % (displayname, line)
        txt_open.close()

        # remove the text file
        os.remove(txt_file)

    # save to csv file
    csv_open = open(csv_file, 'ab')
    csv_open.write(custodiandata.encode('utf-8'))
    csv_open.close()
    os.chmod(csv_file, 0777)


def run(dsi_cases=None):
    # get date for output files
    rundate = str(dat.datetime.today()).split(' ')[0].replace('-', '')

    # bring in the dsi hca case data
    if dsi_cases is None:
        dsi_cases = runrep.getPartnerCases('DSi')

    caselist = sorted(dsi_cases.keys())

    # set paths and output files
    if sys.platform[:3] == 'lin':
        OUTDIR = '/opt/catalyst/insight/reports/dsiReports'

    else:
        OUTDIR = report.get_output_dir('dsi_reports')

    OUTDIR = os.path.join(OUTDIR, 'review_metrics_report')
    if not os.path.exists(OUTDIR):
        os.mkdir(OUTDIR, 0777)

    csv_filename = '%s_%s.csv' % (REPORTNAME, rundate)
    csv_file = os.path.join(OUTDIR, csv_filename)

    if os.path.exists(csv_file):
        os.remove(csv_file)

    # write the header data
    data = 'Project Name,Custodian,Field Name,Field Value,Doc Count\n'
    custodian_save = open(csv_file, 'ab')
    custodian_save.write(data.encode('utf-8'))
    custodian_save.close()
    os.chmod(csv_file, 0777)

    for casename in caselist:
        print casename
        # get collections
        collections = ','.join(dsi_cases[casename].get('collections', ''))
        SEARCHPARAMS['subCollection'] = collections
        displayname = dsi_cases[casename].get('displayname')

        # get case custodians and custodian data
        allcustodians = get_custodianlist(casename)
        print 'Number of custodians: %s' % len(allcustodians.keys())

        # try harder code
        if allcustodians == {}:
            time.sleep(10)
            allcustodians = get_custodianlist(casename)
            print 'Take 2 - number of custodians: %s' % len(allcustodians.keys())

        if allcustodians == {}:
            continue

        # get domain data and save to csv
        get_custodiandata(allcustodians, casename, csv_file, displayname)

    # copy files to ftp location
    if sys.platform[:3] == 'lin':
        runrep.uploadReport(csv_file, '%s/%s' % (REPORTNAME, csv_filename))


if __name__ == '__main__':
    bt = time.clock()

    ds.authenticate(USER, PASS)
    run()

    et = time.clock()
    print round(et - bt, 2)
