#!/opt/catalyst/insight/ve/veMNR_2711/bin/python
import sys
if sys.platform[:3] == 'lin':
    sys.path.append('/opt/catalyst/insight/wopr/app/modules')
else:
    sys.path.append(r'c:\work\repos\mnr\trunk\wopr\app\modules')
from collections import OrderedDict
import catalystTools.spark as spark
import catalystTools.auth as auth


def add_servicename_severity(services, ticket_note):
    # get name of host and service check
    end = ticket_note.find('<br />')
    service_name = ticket_note[:end].replace('<p>', '')

    # if the service_name is not in services dictionary add it
    if service_name not in services.keys():
        services[service_name] = []

    # get alert state and attempts
    startfrom = end
    end = ticket_note.find('</p>')
    alert_severity = ticket_note[startfrom : end].replace('<br />', '')

    # add to the services dictionary
    services[service_name].append(alert_severity.lstrip('\n\r\t '))

    return services


def get_service_data(ticket_data):
    # get the list of ticket history notes
    request_history = ticket_data.get('request_history', [])

    if type(request_history) is list and len(request_history) > 0:
        request_history = reversed(request_history)

    else:
        return {}

    # loop through ticket history list and save data to ordered dictionary
    services = OrderedDict()
    for update in request_history:
        ticket_note = update.get('item', {}).get('tNote', None)
        if ticket_note:
            if '<p>Resources:<br />' in ticket_note:
                services = add_servicename_severity(services, ticket_note)

    return services


def get_summary_data(services):
    data = '### Summary of Checks:  \n'
    is_check_ok = {}
    for service_name, alertlist in services.iteritems():
        # get last state of service names
        last_state = alertlist[-1]
        del alertlist[-1]

        # if the last state is currently OK set to complete
        if 'ok' in last_state.lower() or 'up' in last_state.lower():
            is_check_ok[service_name] = True

        else:
            is_check_ok[service_name] = False

        # go through remainder of alerts and get counts of each iteration
        alert_states = {}
        for alert in alertlist:
            if alert not in alert_states.keys():
                alert_states[alert] = 1

            else:
                alert_states[alert] += 1

        # add to data
        data += '**Check Name: %s**  \n' % service_name
        data += '> **Current state:** *"%s"*  \n' % last_state
        for state, count in alert_states.iteritems():
            data += '> Previous state: "%s" ' % state
            if count == 1:
                data += '- alarmed %s time previously  \n' % count

            else:
                data += ' - alarmed %s times previously  \n' % count

        data += '  \n'

    # add summary to the top:
    top_data = '## Host - Service Status Update:  \n'

    # set the ticket to complete if all checks have cleared
    is_clear = False
    for check in is_check_ok.keys():
        if is_check_ok[check]:
            is_clear = True

        else:
            is_clear = False
            break

    # if all checks are clear state ticket can be closed
    if is_clear:
        top_data += '*All checks are clear.  Ticket can be closed.*\n  \n'
        data = '%s%s' % (top_data, data)

    else:
        # otherwise build a list of the checks that have not cleared
        not_clear = {}
        for check in is_check_ok.keys():
            if not is_check_ok[check]:
                if check not in not_clear.keys():
                    not_clear[check] = 1

                else:
                    not_clear[check] += 1

        # summarize the checks that have not cleared
        top_data += '*Not all checks have cleared.  Please investigate the'
        if len(not_clear.keys()) == 1:
            top_data += ' following %s check:*\n  \n' % len(not_clear.keys())

        else:
            top_data += ' following %s checks:*\n  \n' % len(not_clear.keys())

        for check in sorted(not_clear.keys()):
            num_alerted = not_clear[check]
            if num_alerted == 1:
                top_data += '> %s - %s instance  \n' % (check, num_alerted)

            else:
                top_data += '> %s - %s instances  \n' % (check, num_alerted)

        data = '\n%s%s' % (top_data, data)

    return data, is_clear


