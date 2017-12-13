#! /opt/catalyst/insight/ve/hiatus/bin/python
import os
import sys
import time
import getpass
import argparse
import httplib2
import urllib as ul
import base64 as b64
import socket as sock
from collections import OrderedDict
from datetime import datetime as dat

if sys.platform[:3] == 'lin':
    sys.path.append('/opt/catalyst/insight/wopr/app/modules')
    sys.path.append('/opt/catalyst/er/eric')
else:
    sys.path.append(r'c:\work\repos\mnr\trunk\wopr\app\modules')
    sys.path.append(r'c:\work\repos\mnr\trunk\eric')

import tools.redismod as redmod
import tools.sqlmod as sqlmod
import tools.reports as rep
import sql.hp as hp


class LogFile(object):
    def __init__(self):
        # set local variables
        self.location = rep.get_location()
        year = time.strftime('%Y')
        date = time.strftime('%Y%m%d_%H.%M')

        if self.location == 'us':
            self.country = self.location.upper()
            logurl = 'http://logserver.caseshare.com'
        else:
            self.country = self.location.capitalize()
            logurl = 'http://jplogserver01.crs-tokyo.co.jp'

        self.logname = 'Hiatus_v2-%s_%s' % (self.country, date)
        self.logdir  = '%s/tickettool/sym/hiatus' % logurl
        self.logdir += '/%s/%s.txt' % (year, self.logname)
        self.tt_url = '%s/tt.php?' % logurl
        self.key_logfile = 'Hiatus:LogFile'
        self.key_ticket = 'Hiatus:Ticket'


    def sleep_retry(exception, timeout, maxtry):
        def outer(func):
            def inner(*a, **k):
                i = 0
                while i < maxtry:
                    try:
                        return func(*a, **k)
                    except exception:
                        i += 1
                        time.sleep(timeout)
            return inner
        return outer


    @sleep_retry(httplib2.ServerNotFoundError, 12, 3)
    def log_write(self, message):
        params = {'tx' : self.logname, 'tool' : 'hiatus', 'ts' : 'yes'}
        params['id'] = '%s:%s' % (os.getpid(), sock.gethostname().split('.')[0])
        params['msg'] = str(message).encode('utf-8')
        encoded = ul.urlencode(params)

        return httplib2.Http().request(self.tt_url + encoded, 'GET')[0]['status']


    def set_logfile_ticket(self):
        global TICKET
        red = redmod.RED()
        red.set_key_value(self.key_logfile, self.logdir)
        red.set_key_value(self.key_ticket, TICKET)
        del red


    def get_logfile_ticket(self):
        global TICKET
        red = redmod.RED()
        logdir = red.get_key_value(self.key_logfile)
        TICKET = str(red.get_key_value(self.key_ticket))
        del red

        return logdir


    def __repr__(self):
        return self.logdir


    def __del__(self):
        del self


class SQL_Apps(object):
    def __init__(self, sql_server, sql_database):
        # set local variables
        global DEBUG
        p = hp.get_hp()
        location = rep.get_location()

        # set class variables
        self.sql_server = sql_server
        self.sql_database = sql_database
        self.sql_user = b64.decodestring(p).split(':')[0]
        self.sql_pass = b64.decodestring(p).split(':')[-1]
        self.sql = sqlmod.SQL(self.sql_server, self.sql_database, self.sql_user, self.sql_pass)

        if DEBUG:
            self.machine_app_table = 'MachAppTest'
            self.machine_mast_table = 'MachMastTest'
        else:
            self.machine_app_table = 'MachineApplicationMap'
            self.machine_mast_table = 'MachineMaster'


    def check_sql(self):
        # store and return counts of tables
        counts = {}
        sql_query = "exec dbo.hiatus_select_count 'enabled', %s" % DEBUG
        counts['ApplicationMap'] = self.sql.sql_select(sql_query)[0][0]

        sql_query = "exec dbo.hiatus_select_count 'priority', %s" % DEBUG
        counts['MachineMaster'] = self.sql.sql_select(sql_query)[0][0]

        sql_query = "exec dbo.hiatus_select_count 'appmap', %s" % DEBUG
        counts['AppMapRecover'] = self.sql.sql_select(sql_query)[0][0]

        sql_query = "exec dbo.hiatus_select_count 'master', %s" % DEBUG
        counts['MachMastRecover'] = self.sql.sql_select(sql_query)[0][0]

        return counts


    def pause(self, log, application):
        log.log_write('#### Begin Pause %s ####' % application)

        # check machine counts - save to dictionary - log counts
        pre_app_count = self.check_sql()
        app_map = pre_app_count.get('ApplicationMap')
        mach_mast = pre_app_count.get('MachineMaster')

        log.log_write('%s queue managers currently enabled' % app_map)
        log.log_write('%s priority machines currently enabled' % mach_mast)

        # build query for pausing apps and execute the pause stored procedure
        sql_query  = "exec dbo.hiatus_pause '%s', " % self.machine_app_table
        sql_query += "'%s', '%s'" % (self.machine_mast_table, HIATUS_USER)

        log.log_write('executing dbo.hiatus_pause')
        self.sql.sql_execute(sql_query)
        log.log_write('dbo.hiatus_pause is complete')

        # check machine counts after pause - save to dictionary - log counts
        post_app_count = self.check_sql()
        app_rec = post_app_count.get('AppMapRecover')
        app_map = post_app_count.get('ApplicationMap')
        mach_rec = post_app_count.get('MachMastRecover')
        mach_mast = post_app_count.get('MachineMaster')

        # queue managers
        log.log_write('%s rows of queue manager data backed up' % app_rec)
        log.log_write('%s queue managers currently enabled' % app_map)
        if post_app_count.get('ApplicationMap') == 0:
            log.log_write('Successfully paused queue managers')
        else:
            log.log_write('ERROR pausing queue managers')

        # priority machines
        log.log_write('%s rows of priority machine data backed up' % mach_rec)
        log.log_write('%s priority machines currently enabled' % mach_mast)
        if post_app_count.get('MachineMaster') == 0:
            log.log_write('Successfully paused priority machines')
        else:
            log.log_write('ERROR pausing priority machines')

        # log machine application map data
        log.log_write('The following queue managers were paused')
        sql_query = "exec dbo.hiatus_select_all 'appmap', %s" % DEBUG
        sqldata = self.sql.sql_select(sql_query)

        # write headers first, then loop through each row
        header_row = [str(col[0]) for col in self.sql.cursor.description]
        log.log_write(','.join(header_row))
        for row in sqldata:
            row = [date_convert(col) if type(col) is dat else str(col) for col in row]
            log.log_write(','.join(row))

        # log machine master data
        log.log_write('The following priority machines were paused')
        sql_query = "exec dbo.hiatus_select_all 'master', %s" % DEBUG
        sqldata = self.sql.sql_select(sql_query)

        # write headers first, then loop through each row
        header_row = [str(col[0]) for col in self.sql.cursor.description]
        log.log_write(','.join(header_row))
        for row in sqldata:
            row = [date_convert(col) if type(col) is dat else str(col) for col in row]
            log.log_write(','.join(row))

        log.log_write('#### Pause %s Complete ####' % application)

        return post_app_count


    def start(self, log, application):
        log.log_write('#### Begin Restart %s ####' % application)

        # check machine counts - save to dictionary - log counts
        pre_app_count = self.check_sql()
        app_rec = pre_app_count.get('AppMapRecover')
        mach_rec = pre_app_count.get('MachMastRecover')

        log.log_write('%s queue managers to enable' % app_rec)
        log.log_write('%s priority machines to enable' % mach_rec)

        # check if machines are enabled prior to restart
        if pre_app_count.get('ApplicationMap') > 0:
            log.log_write('The following queue managers were already enabled:')

            # get data
            sql_query = "exec dbo.hiatus_select_all 'enabled', %s" % DEBUG
            sqldata = self.sql.sql_select(sql_query)

            # write headers first, then loop through each row
            header_row = [str(col[0]) for col in self.sql.cursor.description]
            log.log_write(','.join(header_row))
            for row in sqldata:
                row = [date_convert(col) if type(col) is dat else str(col) for col in row]
                log.log_write(','.join(row))

        if pre_app_count.get('MachineMaster') > 0:
            log.log_write('The following priority machines were already enabled:')

            # get data
            sql_query = "exec dbo.hiatus_select_all 'usepriority', %s" % DEBUG
            sqldata = self.sql.sql_select(sql_query)

            # write headers first, then loop through each row
            header_row = [str(col[0]) for col in self.sql.cursor.description]
            log.log_write(','.join(header_row))
            for row in sqldata:
                row = [date_convert(col) if type(col) is dat else str(col) for col in row]
                log.log_write(','.join(row))

        # build query for starting apps and execute the start stored procedure
        sql_query  = "exec dbo.hiatus_start '%s', " % self.machine_app_table
        sql_query += "'%s', '%s'" % (self.machine_mast_table, HIATUS_USER)

        log.log_write('executing dbo.hiatus_start')
        self.sql.sql_execute(sql_query)
        log.log_write('dbo.hiatus_start is complete')

        # check machine counts after pause - save to dictionary - log counts
        post_app_count = self.check_sql()
        app_map = post_app_count.get('ApplicationMap')
        mach_mast = post_app_count.get('MachineMaster')

        # queue managers
        log.log_write('%s queue managers currently enabled' % app_map)
        if post_app_count.get('ApplicationMap') > 0:
            log.log_write('Successfully restarted queue managers')
        else:
            log.log_write('ERROR restarting queue managers')

        # priority machines
        log.log_write('%s priority machines currently enabled' % mach_mast)
        if post_app_count.get('MachineMaster') > 0:
            log.log_write('Successfully restarted priority machines')
        else:
            log.log_write('ERROR restarting priority machines')

        log.log_write('#### Restart %s Complete ####' % application)

        return post_app_count


    def close(self):
        self.sql.close()


    def __repr__(self):
        return '<sql object>'


    def __del__(self):
        del self


class Redis_Apps(object):
    def __init__(self, service=None, applist=None):
        # set local variables
        global DEBUG
        location = rep.get_location()

        # set class variables
        self.service = service
        self.applist = applist
        self.red = redmod.RED(service)
        if self.service == 'caramel':
            self.hiatus_key = 'Hiatus:CaramelBulkOpsKeys'
            self.hiatus_updatedkeys = 'Hiatus:Caramel'
        else:
            self.hiatus_key = 'Hiatus:InboundPipelineKeys'
            self.hiatus_updatedkeys = 'Hiatus:Inbound'

        if DEBUG:
            self.hiatus_key += 'Test'


    def get_all_hiatuskeys(self):
        key_list = self.red.get_key_value(self.hiatus_key).split('|')
        if self.applist is None or self.applist == []:
            return sorted(key_list)
        else:
            return sorted([key for app in self.applist for key in key_list if app in key])


    def get_all_keyvalues(self, all_keys):
        key_values = OrderedDict()
        for key in all_keys:
            key_values[key] = self.red.get_key_value(key)

        return key_values


    def set_all_keyvalues(self, all_keys, value):
        keys_set = []
        for key in all_keys:
            setkey = self.red.set_key_value(key, value)
            if setkey:
                keys_set.append(key)

        return keys_set


    def check_redis(self):
        key_list = self.get_all_hiatuskeys()
        return self.get_all_keyvalues(key_list)


    def pause(self, log):
        # get update values by service
        if self.service.lower() == 'inbound':
            application = 'Inbound Pipeline'
            update_value = 'maintenance%s' % time.strftime('%Y%m%d_%H.%M.%S')
        else:
            application = 'Caramel Bulkops'
            update_value = '1'

        # write preliminary data to log file
        log.log_write('#### Begin Pause %s ####' % application)

        # get redis keys to update
        update_keys = self.get_all_hiatuskeys()

        # get key values
        log.log_write('Current key values:')
        key_values = self.get_all_keyvalues(update_keys)
        for key, val in key_values.iteritems():
            log.log_write('%s = %s' % (key, val))

        # pause application
        log.log_write('Pausing applications')
        setkeys = self.set_all_keyvalues(update_keys, update_value)

        # get key values again
        log.log_write('Current key values after pause:')
        key_values = self.get_all_keyvalues(update_keys)
        for key, val in key_values.iteritems():
            log.log_write('%s = %s' % (key, val))

        # backup keys
        message = 'Backing up keys updated to "%s"' % self.hiatus_updatedkeys
        log.log_write(message)
        self.red.set_key_value(self.hiatus_updatedkeys, '|'.join(update_keys))

        # log completion
        log.log_write('Successfully disabled %s services' % len(update_keys))
        log.log_write('#### Pause %s Complete ####' % application)

        return key_values


    def start(self, log):
        # get update values by service
        if self.service.lower() == 'inbound':
            application = 'Inbound Pipeline'
            update_value = 'go'
        else:
            application = 'Caramel Bulkops'
            update_value = '0'

        # write preliminary data to log file
        log.log_write('#### Begin Restart %s ####' % application)

        # get redis keys to update
        update_keys = self.red.get_key_value(self.hiatus_updatedkeys).split('|')
        update_keys = sorted(update_keys)

        # get key values
        log.log_write('Current key values:')
        key_values = self.get_all_keyvalues(update_keys)
        for key, val in key_values.iteritems():
            log.log_write('%s = %s' % (key, val))

        # restart application
        log.log_write('Restarting applications')
        setkeys = self.set_all_keyvalues(update_keys, update_value)

        # get key values again
        log.log_write('Current key values after restart:')
        key_values = self.get_all_keyvalues(update_keys)
        for key, val in key_values.iteritems():
            log.log_write('%s = %s' % (key, val))

        # log completion
        log.log_write('Successfully restarted %s services' % len(update_keys))
        log.log_write('#### Restart %s Complete ####' % application)

        return key_values


    def __del__(self):
        del self


def date_convert(date):
    # sql date time conversion
    return dat.strftime(date, '%Y-%m-%d %H:%M:%S.%f')


def get_sql_server():
    global DEBUG
    location = rep.get_location().lower()
    if DEBUG:
        server = 'p2sql-dev01'
    else:
        server = 'p2sql1'

    if location == 'japan':
        server = 'j%s' % server

    return server


def check_pause():
    global SERVICES
    if 'dataprocess' in SERVICES:
        sqlapp = SQL_Apps(sqlserver, 'dataprocess')

    elif 'insight' in SERVICES:
        sqlapp = SQL_Apps(sqlserver, 'insight-health')

    app_counts = sqlapp.check_sql()
    if app_counts.get('AppMapRecover') != 0:
        print 'It appears the applications have already been paused.'
        print 'Please run Hiatus restart first.'
        return False
    else:
        return True


def check_restart():
    global SERVICES
    if 'dataprocess' in SERVICES:
        sqlapp = SQL_Apps(sqlserver, 'dataprocess')

    elif 'insight' in SERVICES:
        sqlapp = SQL_Apps(sqlserver, 'insight-health')

    app_counts = sqlapp.check_sql()
    if app_counts.get('AppMapRecover') == 0:
        print 'It appears the applications have already been restarted.'
        print 'Please run Hiatus pause first.'
        return False
    else:
        return True


def update_spark(message):
    global TICKET
    p = hp.get_sp()
    user = b64.decodestring(p).split(':')[0]
    password = b64.decodestring(p).split(':')[-1]
    spark_url  = 'https://zap.caseshare.com/spark/api/index.php?method=private'
    spark_url += '.request.update&username=%s&password=%s' % (user, password)

    params = {}
    params['xRequest'] = TICKET
    params['tNote'] = message

    encoded = ul.urlencode(params)
    result = ul.urlopen(spark_url, encoded)
    if TICKET in result.read():
        return True
    else:
        return False


if __name__ == '__main__':
    bt = time.clock()

    # set global variables
    HIATUS_USER = getpass.getuser()
    if sys.platform[:3] == 'lin':
        # use argparse to get arguments
        parser = argparse.ArgumentParser(description='Hiatus Arguments')

        # required arguments
        parser.add_argument('ticket',
                            type=str,
                            help='Maintenance/deploy ticket number')

        parser.add_argument('pause',
                            type=int,
                            help='1 = pause apps, 0 = restart apps')

        # optional arguments
        help_comment  = '0 (default) = live/production mode'
        help_comment += ' or 1 = debug/test mode'
        parser.add_argument('-d',
                            type=int,
                            dest='debug',
                            default=0,
                            help=help_comment)

        help_comment  = 'comma separated list of services to pause/restart'
        help_comment += ' - ex: inbound,caramel'
        parser.add_argument('-s',
                            dest='service',
                            default=None,
                            help=help_comment)

        help_comment  = 'comma separated list of apps to pause/restart'
        help_comment += ' - ex: carpo,deimos,ganymede'
        parser.add_argument('-a',
                            dest='app',
                            default=None,
                            help=help_comment)

        # assign arguments to variables
        args = parser.parse_args()
        TICKET = args.ticket
        PAUSE = args.pause
        DEBUG = args.debug

        if args.service is None:
            SERVICES = ['dataprocess', 'insight', 'inbound', 'caramel']
        else:
            SERVICES = args.service.split(',')

        APPS = args.app
        if APPS is not None:
            APPS = args.app.split(',')

    else:
        TICKET = '870259'
        PAUSE = 0
        DEBUG = 1
        SERVICES = ['dataprocess', 'insight', 'inbound', 'caramel']
        APPS = []
##        SERVICES = ['inbound']
##        SERVICES = ['caramel']
##        SERVICES = ['inbound', 'caramel']
##        APPS = ['phobos']

    # handle error cases
    if PAUSE not in [1, 0]:
        print 'Pause argument was set to %s.' % PAUSE
        print 'Please set pause to either 1 (pause) or 0 (restart).'
        exit()

    if DEBUG not in [1, 0]:
        print 'Debug argument was set to %s.' % DEBUG
        print 'Setting to Debug = 1 (debug mode).'
        DEBUG = 1

    # get the sql server
    sqlserver = get_sql_server()

    print 'pause: %s' % PAUSE
    print 'debug: %s' % DEBUG
    print 'servs: %s' % SERVICES
    print 'server: %s' % sqlserver

    # initialize logging object
    log = LogFile()

    # logging and data initialization
    if PAUSE:
        # check for data in recovery tables
        if 'dataprocess' in SERVICES or 'insight' in SERVICES:
            # if data exists, exit
            if not check_pause():
                exit()

        # set the redis keys to save logfile dir and ticket number
        log.set_logfile_ticket()
        log.log_write('######## Begin Hiatus %s ########'% log.country)
        log.log_write('Maintenance ticket is %s' % TICKET)
        log.log_write('Hiatus was executed by: %s' % HIATUS_USER)
        log.log_write('###### Pause Applications Begin ######')

        # log beginning of pause
        data  = '## %s - Status after pause:  \n' % log.country
        data += '### Hiatus was executed by: %s  \n' % HIATUS_USER
        data += '[View the Logfile](%s)  \n\n' % log.logdir
    else:
        # check for data in recovery tables
        if 'dataprocess' in SERVICES or 'insight' in SERVICES:
            # if data does not exist, exit
            if not check_restart():
                exit()

        # get the logfile dir and ticket used
        logfile = log.get_logfile_ticket()
        log.logname = logfile.split('/')[-1].replace('.txt', '')
        log.logdir = logfile
        log.log_write('######## Begin Hiatus %s ########'% log.country)
        log.log_write('Hiatus was executed by: %s' % HIATUS_USER)
        log.log_write('###### Restart Applications Begin ######')

        # log beginning of restart
        data  = '## %s - Status after restart:  \n' % log.country
        data += '### Hiatus was executed by: %s  \n' % HIATUS_USER
        data += '[View the Logfile](%s)  \n\n' % log.logdir


    """ data processing """
    if 'dataprocess' in SERVICES:
        # log to output data
        data += '### *Data Processing:*  \n'

        # initialize sql object and pause/restart the application
        sqlapp = SQL_Apps(sqlserver, 'dataprocess')
        app_table = sqlapp.machine_app_table
        mach_table = sqlapp.machine_mast_table
        data += '(%s and %s tables'% (app_table, mach_table)
        data += ' on %s.dataprocess)  \n\n' % sqlserver
        if PAUSE:
            app_counts = sqlapp.pause(log, 'Data Processing')
        else:
            app_counts = sqlapp.start(log, 'Data Processing')
        sqlapp.close()

        # get counts
        app_map = app_counts.get('ApplicationMap', None)
        mach_mast = app_counts.get('MachineMaster', None)
        data += '* Currently enabled queue managers: %s  \n' % app_map
        data += '* Currently enabled priority machines: %s  \n  \n' % mach_mast

    """ insight services """
    if 'insight' in SERVICES:
        # log to output data
        data += '### *Insight Services:*  \n'

        # initialize sql object and pause/restart the application
        sqlapp = SQL_Apps(sqlserver, 'insight-health')
        mach_table = sqlapp.machine_mast_table
        data += '(%s and %s tables'% (app_table, mach_table)
        data += ' on %s.insight-health)  \n\n' % sqlserver
        if PAUSE:
            app_counts = sqlapp.pause(log, 'Insight Services')
        else:
            app_counts = sqlapp.start(log, 'Insight Services')
        sqlapp.close()

        # get counts
        app_map = app_counts.get('ApplicationMap', None)
        mach_mast = app_counts.get('MachineMaster', None)
        data += '* Currently enabled queue managers: %s  \n' % app_map
        data += '* Currently enabled priority machines: %s  \n  \n' % mach_mast

    """ inbound pipeline """
    if 'inbound' in SERVICES:
        # log to output data
        data += '### *Inbound Pipeline:*  \n'
        data += '(enabled = go; disabled = any value but go)  \n\n'

        # initialize redis object and pause/restart the application
        redapp = Redis_Apps('inbound', APPS)
        if PAUSE:
            app_counts = redapp.pause(log)
        else:
            app_counts = redapp.start(log)
        del redapp

        for key, val in app_counts.iteritems():
            data += '* %s = %s  \n' % (key, val)
        data += '  \n'

    """ caramel bulk-ops """
    if 'caramel' in SERVICES:
        # log to output data
        data += '### *Caramel Bulk-ops:*  \n'
        data += '(locks enabled = 1; locks disabled = 0) \n\n '

        # initialize redis object and pause/restart the application
        redapp = Redis_Apps('caramel', APPS)
        if PAUSE:
            app_counts = redapp.pause(log)
        else:
            app_counts = redapp.start(log)
        del redapp

        for key, val in app_counts.iteritems():
            data += '* %s = %s  \n' % (key, val)
        data += '  \n'

    if PAUSE:
        # log pause completion
        log.log_write('###### Pause Applications Complete ######')
        print 'Hiatus pause is complete.'
        print 'View the log file here: %s' % log.logdir
    else:
        # log retart completion
        log.log_write('###### Restart Applications Complete ######')
        print 'Hiatus restart is complete.'
        print 'View the log file here: %s' % log.logdir

    # write completion data to log file and delete the log object
    log.log_write('######## End Hiatus %s ########'% log.country)
    del log

    # update ticket
    update = update_spark(data)
    if not update:
        print 'Ticket number %s was NOT updated.' % TICKET
        print data

    et = time.clock()
    print round(et - bt, 2)
