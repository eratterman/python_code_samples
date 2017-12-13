import sys
if sys.platform[:3] == 'lin':
    sys.path.append('/opt/catalyst/er/eric')
else:
    sys.path.append(r'c:\work\repos\mnr\trunk\wopr\app\modules')
    sys.path.append(r'c:\work\repos\mnr\trunk\eric')
import catalystTools.elasticSearch as es
import catalystTools.caramel as caramel
import catalystTools.auth as auth
import catalystTools.web as web
import tools.reports as report
import tools.sqlmod as sqlmod
import base64 as b64
import requests
import time
import os
import re


def get_job_type(jobtype):
    jobdict = {
                '1' : 'Batch Print', '2' : 'Export', '3' : 'Redaction', '4' : 'Bulk Update', '5' : 'Conversion', '6' : 'OCR',
                '7' : 'Bulk Conversion', '8' : 'Bulk OCR', '9' : 'Production', '10' : 'Folder Batching', '11' : 'Review Project',
                '12' : 'Documents Delete', '13' : 'Power Search', '14' : 'Power Search Add Mutiple Search',
                '15' : 'Partner And Case Provision', '16' : 'Predict Project', '17' : 'Report Export', '18' : 'Review Project FRO',
                '19' : 'Active Review Predict Project', '20' : 'Documents Copy'
                }

    return jobdict.get(str(jobtype), None)


def get_arb_queue(jobtype):
    arb_dict = {
                '1' : 'BatchPrintJobQueue', '2' : 'ExportJobQueue', '3' : 'RedactionJobQueue', '4' : 'BulkUpdateJobQueue',
                '5' : 'ConversionJobQueue', '6' : 'OCRJobQueue', '7' : 'ConversionJobQueue', '8' : 'OCRJobQueue',
                '10' : 'FolderBatch', '11' : 'ReviewProjectJobQueue', '12' : 'DocumentsDeleteJobQueue', '13' : 'PSSearchJobQueue',
                '16' : 'PredictProjectJobQueue', '18' : 'ReviewProjectJobQueue', '19' : 'PredictProjectJobQueue',
                '20' : 'DocumentsCopyJobQueue'
                }

    return arb_dict.get(jobtype, None)


def get_status_type(jobtype, jobstatus):
    if jobtype == '7':
        bulkstatus = {
                      '0' : 'Pending', '1' : 'In progress', '2' : 'Complete', '3' : 'Error',
                      '4' : 'Normalization complete', '5' : 'Conversion submitted to adlib',
                      '6' : 'PDF file exists - overwrite set to false', '7' : 'DPI file written'
                      }
    else:
        bulkstatus = {
                      '0' : 'Pending', '1' : 'In progress', '2' : 'Complete', '3' : 'Error',
                      '4' : 'Split error', '5' : 'OCR error', '6' : 'Join error',
                      '7' : 'General error', '10' : 'Inserting page tasks'
                      }

    return bulkstatus.get(jobstatus, None)


def get_job_data(jobid, user, password):
    esquery = '((jobid : "%s"))' % jobid

    jobs = es.makeGlobalLevelURIRequest('jobs', esquery, 'createdate', 'desc', responseFormat='dict', user=user, password=password)
    jobs = jobs.body.get('hits', {}).get('hits', [])
##    jobs = jobs.get('hits', {}).get('hits', [])

    return jobs[0].get('_source', {})


def get_job_queue_status(jobid, casename, user, password):
    esquery = 'jobid : "%s"' % jobid
    field = ['statusdisplay']
    page_size = 100
    offset = 0

    es_data = es.makeCaseLevelURIRequest(casename, 'jobsqueuestatus', esquery, 'createddate', 'desc', '1', offset, [], 'dict', user, password)
    try:
        hits = es_data.body.get('hits', {}).get('total', 0)
    except:
        return []

    if hits % page_size == 0:
        page_count = hits / page_size
    else:
        page_count = hits / page_size + 1

    es_status = []
    for page in range(0, page_count):
        es_data = es.makeCaseLevelURIRequest(casename, 'jobsqueuestatus', esquery, 'createddate', 'desc', page_size, offset, field, 'dict', user, password)
        es_data = es_data.body.get('hits', {}).get('hits', [])

        for status in es_data:
            es_status.append(status.get('fields', {}).get('statusdisplay')[0])
            offset += 1

    return es_status


def get_arb_status(jobid, jobtype, casename, user, password):
    # set query and arb type
    esquery = 'jobid : "%s"' % jobid
    job_arb = get_arb_queue(jobtype)

    if job_arb is None:
        return None

    # get data
    es_data = es.makeCaseLevelURIRequest(casename, job_arb, esquery, 'createddate', 'desc', '1', '0', ['status'], 'dict', user, password)

    try:
        arb_status = es_data.body.get('hits', {}).get('hits', [])[0]
        return arb_status.get('fields', {}).get('status')[0]
    except:
        return None


def get_complete_message(jobqueue, jobid, jobstatus):
    # set variable
    complete = 'Job complete.'

    # get the job complete status line
    for status in jobqueue:
        if 'complete' in status.lower():
            complete = status
            break

    message  = '* Jobid: %s - **Complete**  \n    ' % jobid
    message += '* Job status: %s  \n    ' % jobstatus
    message += '* Job state: %s  \n  \n' % complete

    return message


def get_status_mismatch(jobid, jobdata, jobqueue, user, password):
    # pull variables from jobdata/jobqueue
    casename = jobdata.get('casename', '')
    jobstatus = jobdata.get('status', '')
    jobtype = jobdata.get('jobtype', '')
    arb_status = get_arb_status(jobid, jobtype, casename, user, password)
    jobarb_type = get_job_type(jobtype)
    jobtype_str = '%s - %s' % (jobtype, jobarb_type)
    assto = jobdata.get('assignedto', '')
    if assto == '':
        assto = 'Not assigned'

    pid = jobdata.get('pid', '')
    if pid == '':
        pid = 'None'

    jobinfo = ''
    for status in jobqueue:
        if 'job info:' in status.lower():
            jobinfo = status
            break

    if jobinfo == '':
        jobinfo = 'Job info: None'

    if len(jobqueue) > 0:
        current = jobqueue[0]
    else:
        current = 'None'

    if len(current) > 200:
        current = current[:200].replace('\n', '').replace('\r', '').replace('\t', '')

    aborted = ''
    for status in jobqueue:
        if 'user aborted' in status.lower():
            aborted = status
            break

    message = ''

    if jobstatus == '2':
        message += get_complete_message(jobqueue, jobid, jobstatus)

    elif aborted != '':
        if arb_status == jobstatus:
            message += '* Jobid: %s - **Status Mismatch Resolved**  \n    ' % jobid
            message += '* Job status: %s  \n    ' % jobstatus
            message += '* %s job arb status: %s\n    ' % (jobarb_type, arb_status)
        else:
            message += '* Jobid: %s - **User Aborted**  \n    ' % jobid
            message += '* Casename: %s  \n    ' % casename
            message += '* Job status: %s  \n    ' % jobstatus
            message += '* %s job arb status: %s\n    ' % (jobarb_type, arb_status)
            message += '* Jobtype: %s  \n    ' % jobtype_str
            message += '* Job state: %s  \n  \n' % aborted

    else:
        message += '* Jobid: %s - **Investigate**  \n    ' % jobid
        message += '* Casename: %s  \n    ' % casename
        message += '* Job status: %s  \n    ' % jobstatus
        message += '* Jobtype: %s  \n    ' % jobtype_str
        message += '* AssignedTo: %s  \n    ' % assto
        message += '* PID: %s  \n    ' % pid
        message += '* %s  \n    ' % jobinfo
        message += '* Job state: %s  \n  \n' % current

    return message


def get_error_message(jobqueue, jobdata, jobstatus):
    error = 'Job error'

    # pull variables from jobdata/jobqueue
    jobid = jobdata.get('jobid', '')
    casename = jobdata.get('casename', '')
    jobstatus = jobdata.get('status', '')
    jobtype = jobdata.get('jobtype', '')
    jobtype_str = '%s - %s' % (jobtype, get_job_type(jobtype))
    subjob = jobdata.get('subjobtype', '')
    predictid = jobdata.get('predictprojectid', '')
    reviewid = jobdata.get('reviewprojectid', '')
    reviewstageid = jobdata.get('reviewstageid', '')
    assto = jobdata.get('assignedto', '')
    if assto == '':
        assto = 'Not assigned'

    pid = jobdata.get('pid', '')
    if pid == '':
        pid = 'None'

    jobinfo = ''
    for status in jobqueue:
        if 'job info:' in status.lower():
            jobinfo = status
            break

    if jobinfo == '':
        jobinfo = 'Job info: None'

    if len(jobqueue) > 0:
        current = jobqueue[0]
    else:
        current = 'None'

    if len(current) > 200:
        current = current[:200].replace('\n', '').replace('\r', '').replace('\t', '')

    # get the error message status line
    for status in jobqueue:
        if 'user aborted' in status.lower():
            error = status
            break
        elif 'error' in status.lower():
            error = status
            if len(error) > 200:
                error = error[:200].replace('\n', '').replace('\r', '').replace('\t', '')
            break

    if 'aborted' in error.lower():
        message  = '* Jobid: %s - **User Aborted**  \n    ' % jobid

    else:
        message  = '* Jobid: %s - **Error**  \n    ' % jobid

    message += '* Casename: %s  \n    ' % casename
    message += '* Job status: %s  \n    ' % jobstatus
    message += '* Job type: %s  \n    ' % jobtype_str
    message += '* Sub-jobtype: %s  \n    ' % subjob
    if jobtype in (['11', '18']) and reviewid != '':
        message += '* Review Project ID: %s  \n    ' % reviewid
        message += '* Review Stage ID: %s  \n    ' % reviewstageid
    elif jobtype in (['16', '18']) and predictid != '':
        message += '* Predict Project ID: %s  \n    ' % predictid

    message += '* AssignedTo: %s  \n    ' % assto
    message += '* PID: %s  \n    ' % pid
    message += '* %s  \n    ' % jobinfo
    message += '* Job state: %s  \n  \n' % current

    return message


def get_simple_status_message(jobid, jobstatus, current):
    message  = '* Jobid: %s  \n    ' % jobid
    message += '* Job status: %s  \n    ' % jobstatus
    message += '* Job state: %s  \n  \n' % current

    return message


def get_aborted_message(jobid, jobstatus, aborted):
    message  = '* Jobid: %s - **User Aborted**  \n    ' % jobid
    message += '* Job status: %s  \n    ' % jobstatus
    message += '* Job state: %s  \n  \n' % aborted

    return message


def get_singleops_message(jobdata, jobqueue):
    # pull variables from jobdata/jobqueue
    jobid = jobdata.get('jobid', '')
    casename = jobdata.get('casename', '')
    jobstatus = jobdata.get('status', '')
    jobtype = jobdata.get('jobtype', '')
    jobtype_str = '%s - %s' % (jobtype, get_job_type(jobtype))
    assto = jobdata.get('assignedto', '')
    if assto == '':
        assto = 'Not assigned'

    pid = jobdata.get('pid', '')
    if pid == '':
        pid = 'None'

    jobinfo = ''
    for status in jobqueue:
        if 'job info:' in status.lower():
            jobinfo = status
            break

    if jobinfo == '':
        jobinfo = 'Job info: None'

    if len(jobqueue) == 0:
        current = 'None'
    else:
        current = jobqueue[0]
        if len(current) > 200:
            current = current[:200].replace('\r', '').replace('\n', '').replace('\t', '')

    message  = ''
    message += '* Jobid: %s  \n    ' % jobid
    message += '* Casename: %s  \n    ' % casename
    message += '* Job status: %s  \n    ' % jobstatus
    message += '* Job type: %s  \n    ' % jobtype_str
    message += '* AssignedTo: %s  \n    ' % assto
    message += '* PID: %s  \n    ' % pid
    message += '* %s  \n    ' % jobinfo
    message += '* Job state: %s  \n  \n' % current

    return message


def get_ml_jobstate(mljobid, casename, user, password):
    joburl = 'prop/job/%s' % mljobid
    jobstate = caramel.makeRequest(casename, joburl, user=user, password=password).body

    if 'cpf:state' in jobstate:
        beg = jobstate.find('name="cpf:state">') + 17
    else:
        beg = jobstate.find('/states/') + 8
    end = jobstate.find('</property>', beg)

    return jobstate[beg : end]


def get_marklogic_jobids(jobqueue, casename, user, password):
    ml_list = []
    ml_family = []
    for status in jobqueue:
        status = status.lower()
        if 'error' in status or 'failed' in status:
            continue

        # search each line for specific words
        if 'caramel bulkops' in status and 'total' not in status:
            ml_list.append(re.sub('[^0-9]', '', status[status.find('caramel bulkops') :]))

        if 'mljobid' in status:
            ml_list.append(re.sub('[^0-9]', '', status[: status.find(',')]))

        if 'mlfamilyjobid' in status:
            ml_family.append(re.sub('[^0-9]', '', status[status.find('mlfamilyjobid: ') :]))

        if 'ml job id' in status:
            joblist = re.sub('[^0-9,]', '', status).split(',')
            for job in joblist:
                ml_list.append(job)

    # get the states of the mark logic jobs
    if len(ml_list) > 0:
        ml_list = sorted(list(set(ml_list)))

        mljobids = []
        for mljobid in ml_list:
            jobstate = get_ml_jobstate(mljobid, casename, user, password)
            mljobids.append('%s - status: "%s"' % (mljobid, jobstate))

        mljobids = '; '.join(mljobids)
    else:
        mljobids = None

    if len(ml_family) > 0:
        ml_family = sorted(list(set(ml_family)))

        mlfamilies = []
        for mljobid in ml_family:
            jobstate = get_ml_jobstate(mljobid, casename, user, password)
            mlfamilies.append('%s - status: "%s"' % (mljobid, jobstate))

        mlfamilies = '; '.join(mlfamilies)
    else:
        mlfamilies = None

    return mljobids, mlfamilies


def get_reviewproject_message(jobdata, jobqueue, user, password):
    # pull variables from jobdata/jobqueue
    jobid = jobdata.get('jobid', '')
    jobstatus = jobdata.get('status', '')
    casename = jobdata.get('casename', '')
    joburi = jobdata.get('localjobURI', '')
    subjob = jobdata.get('subjobtype', '')
    reviewid = jobdata.get('reviewprojectid', '')
    reviewstageid = jobdata.get('reviewstageid', '')
    jobtype = jobdata.get('jobtype', '')
    current = jobqueue[0]
    jobtype_str = '%s - %s' % (jobtype, get_job_type(jobtype))

    assto = jobdata.get('assignedto', '')
    if assto == '':
        assto = 'Not assigned'

    pid = jobdata.get('pid', '')
    if pid == '':
        pid = 'None'

    # get ml job states
    mljobid, mlfamily = get_marklogic_jobids(jobqueue, casename, user, password)

    message  = ''
    message += '* Jobid: %s  \n    ' % jobid
    message += '* Local jobid: %s  \n    ' % joburi
    message += '* Casename: %s  \n    ' % casename
    message += '* Status: %s  \n    ' % jobstatus
    message += '* Job type: %s  \n    ' % jobtype_str
    message += '* Sub-jobtype: %s  \n    ' % subjob
    if jobtype in ('11', '18'):
        message += '* Review Project ID: %s  \n    ' % reviewid
        message += '* Review Stage ID: %s  \n    ' % reviewstageid

    message += '* AssignedTo: %s - PID: %s  \n    ' % (assto, pid)
    message += '* Status display: %s  \n' % current

    if mljobid is not None:
        message += '    * ML jobid: %s  \n' % mljobid

    if mlfamily is not None:
        message += '    * ML family jobid: %s  \n' % mlfamily

    message += '  \n'

    return message


def get_deletejob_message(jobdata, jobqueue):
    # pull variables from jobdata/jobqueue
    jobid = jobdata.get('jobid', '')
    casename = jobdata.get('casename', '')
    jobstatus = jobdata.get('status', '')
    jobtype = jobdata.get('jobtype', '')
    jobtype_str = '%s - %s' % (jobtype, get_job_type(jobtype))
    subjobtype = jobdata.get('subjobtype', '')
    doccount = jobdata.get('totaldocuments', '')
    joburi = jobdata.get('localjobURI', '')

    assto = jobdata.get('assignedto', '')
    if assto == '':
        assto = 'Not assigned'

    pid = jobdata.get('pid', '')
    if pid == '':
        pid = 'None'

    jobinfo = ''
    for status in jobqueue:
        if 'job info:' in status.lower():
            jobinfo = status
            break

    if jobinfo == '':
        jobinfo = 'Job info: None'

    if subjobtype == '1':
        subjobtype = '%s - delete' % subjobtype
    else:
        subjobtype = '%s - archive' % subjobtype

    if len(jobqueue) > 0:
        current = jobqueue[0]
    else:
        current = 'None'

    if len(current) > 150:
        current = current[:150].replace('\n', '').replace('\r', '').replace('\t', '')

    message  = ''
    message += '* Jobid: %s  \n    ' % jobid
    message += '* Casename: %s  \n    ' % casename
    message += '* Job status: %s  \n    ' % jobstatus
    message += '* Job type: %s  \n    ' % jobtype_str
    message += '* Sub-jobtype: %s  \n    ' % subjobtype
    message += '* AssignedTo: %s  \n    ' % assto
    message += '* PID: %s  \n    ' % pid
    message += '* %s  \n    ' % jobinfo
    message += '* Job state: %s  \n  \n' % current

    return message


def get_simplex_status(dre_ids):
    dre_status = {}
    for dre_id in dre_ids:
        if report.get_location() == 'us':
            dre_url = 'http://dre-ops01.caseshare.com/job/%s' % dre_id
        else:
            dre_url = 'http://dre-ops01-jp.crs-tokyo.co.jp/job/%s' % dre_id
        r = requests.get(dre_url)

        beg = r.text.find("u&#39;status&#39;:") + 25
        end = r.text.find('&#39;}')
        status = r.text[beg : end]
        dre_status[dre_id] = status

    return dre_status


def get_drejob_message(jobdata, jobqueue):
    # pull variables from jobdata/jobqueue
    jobid = jobdata.get('jobid', '')
    jobstatus = jobdata.get('status', '')
    casename = jobdata.get('casename', '')
    jobtype = jobdata.get('jobtype', '')
    jobtype_str = '%s - %s' % (jobtype, get_job_type(str(jobtype)))
    joburi = jobdata.get('localjobURI', '')
    subjob = jobdata.get('subjobtype', '')
    predictid = jobdata.get('predictprojectid', '')
    current = jobqueue[0]

    if len(current) > 250:
        current = current[:200].replace('\n', '').replace('\r', '').replace('\t', '')

    dre_ids = []
    for status in jobqueue:
        if 'simplex job' in status.lower():
            if jobtype == '19':
                dre_id = status[status.find('simplex job id:') + 16: status.find(', round')]
            else:
                dre_id = status[status.find('job_id - ') + 9: ]

            if not dre_id.isdigit():
                dre_id = 'None'
                continue

            dre_ids.append(dre_id)

    if dre_ids != []:
        dre_status = get_simplex_status(dre_ids)

    message  = ''
    message += '* Jobid: %s  \n    ' % jobid
    message += '* Local jobid: %s  \n    ' % joburi
    message += '* Casename: %s  \n    ' % casename
    message += '* Status: %s  \n    ' % jobstatus
    message += '* Jobtype: %s  \n    ' % jobtype_str
    message += '* Sub-jobtype: %s  \n    ' % subjob
    message += '* Predict project ID: %s  \n    ' % predictid
    message += '* Status display: %s  \n' % current
    if dre_ids == []:
        message += '    * No Simplex jobs created  \n'
    else:
        for dre_id, status in dre_status.iteritems():
            message += '    * Simplex jobid: %s  \n' % dre_id
            message += '    * Simplex status: %s  \n' % status

    message += '  \n'

    return message


def get_bulkconversion_status(joburi, casename):
    # set query
##    query  = "SELECT Status, count(1) AS Count FROM PDFConversion "
##    query += "WITH (NOLOCK) WHERE id3 = %s AND casename " % joburi
##    query += "= '%s' GROUP BY Status ORDER BY Status" % casename

    query = "EXEC dbo.GetConversionStatus '%s', %s" % (casename, joburi)

    # create sql connection object and get data
    sql = sqlmod.SQL('p2sql1', 'Conversion', app='sql-mnr')
    bulkjob_status = sql.sql_select(query)
    sql.close()

    if bulkjob_status is None or bulkjob_status == []:
        return None
    else:
        return bulkjob_status


def get_bulkocr_status(jobid):
    # set query
##    query  = "SELECT Status, count(1) AS Count FROM OCRDOCUMENT WITH "
##    query += "(NOLOCK) WHERE GeneratedByApp = 'InsightBulkOCRJob-"
##    query += "%s' GROUP BY Status ORDER BY STATUS" % jobid

    query = "EXEC dbo.GetOCRStatus '%s'" % jobid

    # create sql connection object and get data
    sql = sqlmod.SQL('p2sql1', 'OCR', app='sql-mnr')
    bulkjob_status = sql.sql_select(query)
    sql.close()

    if bulkjob_status is None or bulkjob_status == []:
        return None
    else:
        return bulkjob_status


def get_redaction_status(jobid):
    # set query
    query = "EXEC dbo.GetRedactionStatus"

    # create sql connection object and get data
    sql = sqlmod.SQL('p2sql1', 'redaction', app='sql-mnr')
    bulkjob_status = sql.sql_select(query)
    sql.close()

    if bulkjob_status is None or bulkjob_status == []:
        return None
    else:
        return bulkjob_status


def get_bulkops_message(jobdata, jobqueue):
    # pull variables from jobdata/jobqueue
    jobid = jobdata.get('jobid', '')
    casename = jobdata.get('casename', '')
    jobstatus = jobdata.get('status', '')
    jobtype = jobdata.get('jobtype', '')
    joburi = jobdata.get('localjobURI', '')
    jobtype_str = '%s - %s' % (jobtype, get_job_type(str(jobtype)))
    assto = jobdata.get('assignedto', '')
    if assto == '':
        assto = 'Not assigned'

    pid = jobdata.get('pid', '')
    if pid == '':
        pid = 'None'

    current = jobqueue[0]
    if 'this job is currently running' in current.lower():
        current = current[:80]

    jobinfo = ''
    for status in jobqueue:
        if 'job info:' in status.lower():
            jobinfo = status
            break

    if jobinfo == '':
        jobinfo = 'Job info: None'

    if jobtype == '3':
        bulkjob_status = get_redaction_status(jobid)
    elif jobtype == '7':
        bulkjob_status = get_bulkconversion_status(joburi, casename)
    else:
        bulkjob_status = get_bulkocr_status(jobid)

    message  = ''
    message += '* Jobid: %s  \n    ' % jobid
    message += '* Casename: %s  \n    ' % casename
    message += '* %s  \n    ' % jobinfo
    message += '* Job status: %s  \n    ' % jobstatus
    message += '* AssignedTo: %s - PID: %s  \n    ' % (assto, pid)
    message += '* Status display: %s  \n' % current

    if bulkjob_status is not None:
        message += '    * SQL task status:  \n'
        for bulkjob in bulkjob_status:
            status = str(bulkjob[0])
            count = bulkjob[-1]
            status_str = get_status_type(jobtype, status)
            message += '        * Status: %s - Count: %s' % (status, count)
            message += ' (%s)  \n' % status_str

    message += '  \n'

    return message


def get_job_details(joblist, user, password, simplestatus=False, mismatch=False):
    message = '### Job Status Update:  \n'

    for jobid in joblist:
        aborted = ''

        # get the job details and other variables
        jobdata = get_job_data(jobid, user, password)
        casename = jobdata.get('casename')
        jobstatus = str(jobdata.get('status'))
        jobtype = str(jobdata.get('jobtype'))
        subjobtype = jobdata.get('subjobtype')

        # get job queue status data sorted by descending and first/last message
        jobqueue = get_job_queue_status(jobid, casename, user, password)
        if len(jobqueue) > 0:
            current = jobqueue[0]
        else:
            current = 'None'

        if mismatch:
            message += get_status_mismatch(jobid, jobdata, jobqueue, user, password)
            continue

        if jobstatus == '2':
            message += get_complete_message(jobqueue, jobid, jobstatus)
            continue

        if jobstatus == '3':
            message += get_error_message(jobqueue, jobdata, jobstatus)
            continue

        if simplestatus:
            message += get_simple_status_message(jobid, jobstatus, current)
            continue

        # check for user aborted jobs
        for status in jobqueue:
            if 'user aborted' in status.lower():
                aborted = status
                break

        if aborted != '':
            message += get_aborted_message(jobid, jobstatus, aborted)
            continue

##        if jobtype in ['1', '2', '3', '5', '9', '17', '20']:
        if jobtype in ['1', '2', '5', '9', '17', '20']:
            message += get_singleops_message(jobdata, jobqueue)

        elif jobtype in ['4', '11', '18']:
            message += get_reviewproject_message(jobdata, jobqueue, user, password)

        elif jobtype == '12':
            message += get_deletejob_message(jobdata, jobqueue)

        elif jobtype in ['16', '19']:
            message += get_drejob_message(jobdata, jobqueue)

        elif jobtype in ['3', '7', '8']: # if issues remove jobtype 3
            message += get_bulkops_message(jobdata, jobqueue)

    return message


def convert_message_to_html(message):
    outmessage = message.split('\n')

    data = []
    data.append('<ul style="list-style: square;">')
    for line in outmessage:
        line = line.strip()
        if line == '':
            continue

        if '###' in line:
            line_split = line.split('###')
            line = '<strong>%s</strong>' % line_split[1].strip()

        else:
            line = line.replace(' - **', ' - <strong>').replace('**', '</strong>').strip()
            line = line.replace('* ', '').strip()

        data.append('<li>%s</li>' % line)

    data.append('</ul>')

    return data


def update_spark(ticketnumber, message, email=None, sparkpass=None):
    if email is None:
        email = auth.getAppUser('spark')

    if sparkpass is None:
        sparkpass = auth.getAppPassword('spark')

    url  = 'https://zap.caseshare.com/spark/api/index.php?'
    url += 'method=private.request.update'
    url += '&username=%s&password=%s' % (email, sparkpass)

    params = {}
    params['fOpen'] = 1
    params['xRequest'] = ticketnumber
    params['tNote'] = message

    encoded = web.urlEncode(params)
    result = web.makeRequest(url, body=encoded, responseFormat='string').body

    if '<xRequest>%s</xRequest>' % ticketnumber in result:
        return True
    else:
        return False

