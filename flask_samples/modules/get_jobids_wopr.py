import sys
sys.path.append('/opt/catalyst/insight/wopr/app/modules')
import catalystTools.elasticSearch as es
import catalystTools.auth as auth
import catalystTools.jam as jam
import clear_PSD as clear
import base64 as b64
import re


BULKOPS = ['4', '10', '11', '13', '18']


# ============================================ #
# get global jobids
# ============================================ #

def get_mljobid_queue_status(mljobid, casename, user, password):
    fields = ['statusdisplay', 'jobid']
    page_size = 10000
    offset = 0

    es_data = es.makeCaseLevelURIRequest(casename, 'jobsqueuestatus', mljobid, 'createddate', 'desc', '1', offset, [], 'dict', user, password)
    hits = es_data.body.get('hits', {}).get('total', 0)

    if hits % page_size == 0:
        page_count = hits / page_size
    else:
        page_count = hits / page_size + 1

    es_status = []
    for page in range(0, page_count):
        es_data = es.makeCaseLevelURIRequest(casename, 'jobsqueuestatus', mljobid, 'createddate', 'desc', page_size, offset, fields, 'dict', user, password)
        es_data = es_data.body.get('hits', {}).get('hits', [])

        for status in es_data:
            jobid = status.get('fields', {}).get('jobid')[0]
            statusline = status.get('fields', {}).get('statusdisplay')[0]
            es_status.append({'jobid' : jobid, 'status' : statusline})
            offset += 1

    return es_status


def get_global_jobid(mljobid, casename, user, password):
    job_status = get_mljobid_queue_status(mljobid, casename, user, password)

    jobid = None

    if not job_status:
        return jobid

    for job in job_status:
        jobstatus = job.get('status').lower()

        if 'caramel bulkops jobid: %s' % mljobid in jobstatus:
            jobid = job.get('jobid')

        if 'mljobid: %s' % mljobid in jobstatus:
            jobid = job.get('jobid')

        if 'mlfamilyjobid: %s' % mljobid in jobstatus:
            jobid = job.get('jobid')

        if 'ml job id' in jobstatus:
            jobid = job.get('jobid')

    return jobid


def get_job_data(jobid, user, password):
    esquery = '((jobid : "%s"))' % jobid

    jobs = es.makeGlobalLevelURIRequest('jobs', esquery, 'createdate', 'desc', responseFormat='dict', user=user, password=password)
    jobs = jobs.body.get('hits', {}).get('hits', [])

    try:
        return jobs[0].get('_source', {})
    except:
        return None


def get_job_type(jobtype):
    jobdict = {'1' : 'Batch Print', '2' : 'Export', '3' : 'Redaction',
                '4' : 'Bulk Update', '5' : 'Conversion', '6' : 'OCR',
                '7' : 'Bulk Conversion', '8' : 'Bulk OCR', '9' : 'Production',
                '10' : 'Folder Batching', '11' : 'Review Project',
                '12' : 'Documents Delete', '13' : 'Power Search',
                '14' : 'Power Search Add Mutiple Search',
                '15' : 'Partner And Case Provision', '16' : 'Power Search',
                '17' : 'Report Export', '18' : 'Review Project FRO',
                '19' : 'Active Review Predict Project'}

    return '%s - %s' % (str(jobtype), jobdict[str(jobtype)])


def get_cluster(casename):
    casedata = clear.ins_cases_dict()

    return casedata.get(casename).get('cluster')


def get_global_job_details(jobid, mljobid, casename, user, password):
    jobsqueue = get_job_data(jobid, user, password)
    jobtype = jobsqueue.get('jobtype')

    job_data = []
    job_data.append('<ul style="list-style: square;">')
    job_data.append('<strong><li>Global Jobid: %s</strong></li>' % jobid)
    job_data.append('<li>Casename: %s</li>' % casename)
    job_data.append('<li>Job status: %s</li>' % jobsqueue.get('status', ''))
    job_data.append('<li>Jobtype: %s</li>' % get_job_type(jobtype))
    job_data.append('<li>Sub-jobtype: %s</li>' % jobsqueue.get('subjobtype', ''))
    job_data.append('<li>Cluster: %s</li>' % get_cluster(casename))
    job_data.append('<li>Mark Logic jobid: %s</li>' % mljobid)
    job_data.append('<li>Mark Logic jobURI: /case/%s/prop/job/%s</li>' % (casename, mljobid))
    job_data.append('<li>Total documents: %s</li>' % jobsqueue.get('totaldocuments', ''))
    job_data.append('<li>Jobs v2: %s</li>' % jobsqueue.get('v2', ''))
    job_data.append('<li>Created by: %s</li>' % jobsqueue.get('createdby', ''))
    job_data.append('<li>Create date: %s</li>' % jobsqueue.get('createdate', ''))
    job_data.append('<li>Start date: %s</li>' % jobsqueue.get('startdate', ''))
    job_data.append('<li>End date: %s</li>' % jobsqueue.get('enddate', ''))
    job_data.append('<li>Workflow comment: %s</li>' % jobsqueue.get('workflowcomment', ''))
    job_data.append('<li>Error info: %s</li></ul>' % jobsqueue.get('errorinfo', ''))

    return job_data

# ============================================ #
# get mark logic jobids
# ============================================ #

def get_job_queue_status(jobid, casename, user, password):
    esquery = 'jobid : "%s"' % jobid
    field = ['statusdisplay']
    page_size = 100
    offset = 0

    es_data = es.makeCaseLevelURIRequest(casename, 'jobsqueuestatus', esquery, 'createddate', 'desc', '1', offset, [], 'dict', user, password)
    hits = es_data.body.get('hits', {}).get('total', 0)

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


def get_job_details(jobdata, user, password):
    jobid = jobdata.get('jobid', '')
    casename = jobdata.get('casename', '')
    jobtype = jobdata.get('jobtype', '')

    job_data = []
    job_data.append('<ul style="list-style: square;">')

    # handle jobtypes that do not create jobids
    if jobtype in BULKOPS:
        # get the jobs queue status entries for the jobid
        es_status = get_job_queue_status(jobid, casename, user, password)

        # pull out the mark logic jobids from the status messages
        ml_list = []
        ml_family = []
        for status in es_status:
            status = status.lower()

            if 'error' in status:
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

        if ml_list:
            # remove dups and sort ascending
            ml_list = sorted(list(set(ml_list)))

            # add ml list to job_dict
            if len(ml_list) == 0:
                job_data.append('<li><strong>No Mark Logic jobids found</strong></li>')

            elif len(ml_list) == 1:
                job_data.append('<li><strong>Mark Logic jobid: %s</strong></li>' % ml_list[0])
                joburi = '/case/%s/prop/job/%s' % (casename, ml_list[0])
                job_data.append('<li>Mark Logic jobURI: %s</li>' % joburi)

            else:
                joburi_list = []
                for joburi in ml_list:
                    joburi_list.append('/case/%s/prop/job/%s' % (casename, joburi))
                job_data.append('<li><strong>Mark Logic jobids: %s</strong></li>' % ', '.join(ml_list))
                job_data.append('<li>Mark Logic jobURIs: %s</li>' % ', '.join(joburi_list))

        if ml_family:
            # remove dups and sort ascending
            ml_family = sorted(list(set(ml_family)))

            # add ml family list unless there was none
            if len(ml_family) == 1:
                job_data.append('<li><strong>Mark Logic family jobid: %s</strong></li>' % ml_family[0])
                joburi = '/case/%s/prop/job/%s' % (casename, ml_family[0])
                job_data.append('<li>Mark Logic family jobURI: %s</li>' % joburi)

            elif len(ml_family) > 1:
                joburi_list = []
                for joburi in ml_family:
                    joburi_list.append('/case/%s/prop/job/%s' % (casename, joburi))
                job_data.append('<li><strong>Mark Logic family jobids: %s</strong></li>' % ', '.join(ml_family))
                job_data.append('<li>Mark Logic family jobURIs: %s</li>' % ', '.join(joburi_list))

    else:
        job_data.append('<li><strong>Jobtype %s does not create a Mark Logic job</strong></li>' % jobtype)

    # add remaining job data
    job_data.append('<li>Global jobid: %s</li>' % jobid)
    job_data.append('<li>Casename: %s</li>' % casename)
    job_data.append('<li>Jobtype: %s</li>' % get_job_type(jobtype))
    job_data.append('<li>Cluster: %s</li></ul>' % get_cluster(casename))

    return job_data


if __name__ == '__main__':
    pass

##    user = auth.getAppUser(APP)
##    password = auth.getAppPassword(APP)
##    print jam.dataService.authenticate(USER, PASS)

# ============================================ #
# get global jobids
# ============================================ #

##    try:
##        joburi = '/case/insdsihcaherrera/prop/job/668'
##        mljobid = joburi.split('/')[-1]
##        if joburi.split('/')[1].lower() == 'case':
##            casename = joburi.split('/')[2]
##
##        elif joburi.split('/')[0].lower() == 'case':
##            casename = joburi.split('/')[1]
##
##        else:
##            print 'To run this script add the joburi starting with the /case/:'
##            print '/opt/catalyst/er/ml_jobs/get_global_jobids.py /case/insdsihcaherrera/prop/job/668'
##            exit()
##    except:
##        print 'To run this script add the joburi starting with the /case/:'
##        print '/opt/catalyst/er/ml_jobs/get_global_jobids.py /case/insdsihcaherrera/prop/job/668'
##        exit()
##
##
##    jobid = get_global_jobid(mljobid, casename, user, password)
##
##    if jobid:
##        print get_global_job_details(jobid, mljobid, casename, user, password)
##    else:
##        print 'Unable to locate the Mark Logic jobid %s on the %s case.'


# ============================================ #
# get mark logic jobids
# ============================================ #


##    try:
##        jobid = '2292631'
####        jobid = sys.argv[1]
##    except:
##        print 'To run this script add the jobid at the end of the command:'
##        print '/opt/catalyst/er/ml_jobs/get_ml_jobids.py 1600805'
##        exit()
##
##    jobdata = get_job_data(jobid, user, password)
##    ml_jobdata = get_job_details(jobdata, user, password)
##    print ml_jobdata


