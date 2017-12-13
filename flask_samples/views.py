from flask import Flask, render_template, flash, redirect, url_for, request, make_response, session, g, make_response
from flask.ext.login import login_user, logout_user, current_user, login_required
from app import app, login_manager
import forms
import sys
import os.path
import re
import json
import traceback
from models import User
from datetime import datetime
import time, threading, socket
from operator import itemgetter
import werkzeug
import requests

import job_status_wopr as wopr_jobs
import get_jobids_wopr as wopr_jobids
import event_relay_summary as event_relay

# a few samples from views.py

@app.route('/insight_job_status', methods = ['GET', 'POST'])
@login_required
def insight_job_status():
    # import insight_job_status as wopr_jobs

    if not permissionCheck('insight_job_status'):
        return redirect(url_for('index'))

    mod_name = 'insight_job_status'
    user = current_user.username
    email = current_user.email
    password = session.get('password', None)
    sparkpass = session.get('sparkpassword', None)
    session['send_back_to'] = mod_name
    form = forms.insight_job_status()

    if 'sparkpassword' not in session.keys():
        flash('Please type in your spark password', 'alert')
        return redirect(url_for('profile'))
    else:
        flash("Let's check some job status!", 'alert')

    if request.method == 'GET':
        return render_template('insight_job_status.html', form=form)

    if request.method == 'POST':
        if form.validate_on_submit():
            flash('Getting job status', 'info')

            ticketnumber = form.ticketnumber.data
            simplestatus = eval(form.simplestatus.data)
            mismatch = eval(form.mismatch.data)
            jobids = form.jobids.data
            jobids = re.sub('[^0-9,]', '', jobids).lstrip(',').rstrip(',').split(',')
            jobids = sorted(list(set(jobids)))

            message = wopr_jobs.get_job_details(jobids, user, password, simplestatus, mismatch)

            if ticketnumber is None or ticketnumber == '':
                message = wopr_jobs.convert_message_to_html(message)

                flash('Copy and paste below results into the ticket', 'success')
                return render_template('insight_job_status.html', form=False, message=message)

            else:
                update_spark = wopr_jobs.update_spark(ticketnumber, message, email, sparkpass)

                flash('Results were sent to ticket number "%s"' % ticketnumber, 'success')
##                return render_template('insight_job_status.html', form='none')
                return render_template('insight_job_status.html', form=form)

        else:
            flash('Something went awry. Please try again.', 'error')
            return render_template('insight_job_status.html', form=form)


@app.route('/get_global_jobid', methods = ['GET', 'POST'])
@login_required
def get_global_jobid():
    # import job_status_wopr as wopr_jobs

    if not permissionCheck('get_global_jobid'):
        return redirect(url_for('index'))

    mod_name = 'get_global_jobid'
    user = current_user.username
    password = session.get('password', None)
    session['send_back_to'] = mod_name
    form = forms.get_global_jobid()

    flash("Let's find some global jobids!", 'alert')

    if request.method == 'GET':
        return render_template('get_global_jobid.html', form=form)

    if request.method == 'POST':
        if form.validate_on_submit():
            flash('Getting jobid', 'info')
            joburi = form.ml_joburi.data

            if joburi:
                mljobid = joburi.split('/')[-1]
                if joburi.split('/')[1].lower() == 'case':
                    casename = joburi.split('/')[2]

                elif joburi.split('/')[0].lower() == 'case':
                    casename = joburi.split('/')[1]

                else:
                    flash('Please enter a valid ML joburi: EX: /case/insdsihcaherrera/prop/job/668', 'error')
                    return render_template('get_global_jobid.html', form=form)

            else:
                flash('Please enter a valid ML joburi: EX: /case/insdsihcaherrera/prop/job/668', 'error')
                return render_template('get_global_jobid.html', form=form)

            # get the global jobid
            jobid = wopr_jobids.get_global_jobid(mljobid, casename, user, password)

            # get the jobdata or return to page to try again.
            if not jobid:
                flash('Unable to locate jobid.  Please try again.', 'error')
                return render_template('get_global_jobid.html', form=form)

            jobdata = wopr_jobids.get_global_job_details(jobid, mljobid, casename, user, password)

            flash('Congratulations!  We found the global job data.', 'success')
            return render_template('get_global_jobid.html', form=False, jobdata=jobdata)
        else:
            flash('Something went awry. Please try again.', 'error')
            return render_template('get_global_jobid.html', form=form)


@app.route('/get_ml_jobid', methods = ['GET', 'POST'])
@login_required
def get_ml_jobid():
    # import get_jobids_wopr as wopr_jobids
    if not permissionCheck('get_ml_jobid'):
        return redirect(url_for('index'))

    mod_name = 'get_ml_jobid'
    user = current_user.username
    password = session.get('password', None)
    session['send_back_to'] = mod_name
    form = forms.get_ml_jobid()

    flash("Let's find some Mark Logic jobids!", 'alert')

    if request.method == 'GET':
        return render_template('get_ml_jobid.html', form=form)

    if request.method == 'POST':
        if form.validate_on_submit():
            flash('Getting Mark Logic jobids', 'info')
            jobid = form.global_jobid.data

            jobdata = wopr_jobids.get_job_data(jobid, user, password)
            # get the jobdata or return to page to try again.
            if not jobdata:
                flash('Unable to locate jobdata from the jobid.  Please try again.', 'error')
                return render_template('get_global_jobid.html', form=form)

            ml_jobdata = wopr_jobids.get_job_details(jobdata, user, password)

            flash('Congratulations!  We found the Mark Logic jobid.', 'success')
            return render_template('get_ml_jobid.html', form=False, ml_jobdata=ml_jobdata)

        else:
            flash('Something went awry. Please try again.', 'error')
            return render_template('get_ml_jobid.html', form=form)


@app.route('/event_relay_summary', methods = ['GET', 'POST'])
@login_required
def event_relay_summary():
    # import event_relay_summary as event_relay
    if not permissionCheck('event_relay_summary'):
        return redirect(url_for('index'))

    mod_name = 'event_relay_summary'
    email = current_user.email
    sparkpass = session.get('sparkpassword', None)
    session['send_back_to'] = mod_name
    form = forms.event_relay_summary()

    if not sparkpass:
        flash('Please type in your spark password', 'alert')
        return redirect(url_for('profile'))

    else:
        flash('Event Relay Ticket Summary', 'alert')

    if request.method == 'GET':
        return render_template('event_relay_summary.html', form=form)

    if request.method == 'POST':
        if form.validate_on_submit():
            ticketnumber = form.ticketnumber.data
            app.logger.info('ticket: %s' % ticketnumber)

            if ticketnumber is None:
                flash('Please enter a valid ticket number.', 'error')
                return render_template('event_relay_summary.html', form=form)

            # get the ticket data
            ticket_data = spark.getSparkTicket(ticketnumber, email, sparkpass)
            ticket_data = ticket_data.get('request', {})

            # get the current ticket status
            ticket_status = ticket_data.get('xStatus', None)
            ticket_state = ticket_data.get('fOpen', None)

            # get ticket host and service data
            services = event_relay.get_service_data(ticket_data)

            # summarize the checks
            data, set_complete = event_relay.get_summary_data(services)

            # update spark ticket
            params = {}
            params['tNote'] = data
            params['fOpen'] = ticket_state

            spark.updateSparkTicket(ticketnumber, params, email, sparkpass)

            flash('Please check ticket number %s for the summary.' % ticketnumber, 'success')
            flash('Would you like to check another ticket?', 'info')
            return render_template('event_relay_summary.html', form=form)

        else:
            flash('Something went awry. Please try again.', 'error')
            return render_template('event_relay_summary.html', form=form)

