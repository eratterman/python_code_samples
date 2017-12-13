from flask.ext.wtf import Form
from wtforms import TextField, BooleanField, PasswordField, SelectField, RadioField, HiddenField, SelectMultipleField, widgets as wid
from wtforms.validators import Required, NumberRange, Regexp, AnyOf


class insight_job_status(Form):
    ticketnumber = TextField('ticketnumber')
    jobids = TextField('jobids', validators=[Required()])
    simplestatus = RadioField('simplestatus', choices=[('True', 'Simple Status'), ('False', 'Detailed Status')], validators=[Required()], default='False')
    mismatch = RadioField('mismatch', choices=[('True', 'Yes'), ('False', 'No')], validators=[Required()], default='False')


class get_global_jobid(Form):
    ml_joburi = TextField('ml_joburi', validators=[Required()])


class get_ml_jobid(Form):
    global_jobid = TextField('global_jobid', validators = [Required(), Regexp('^[0-9]*$', flags=0, message=u'Field must be an integer')])


class event_relay_summary(Form):
    ticketnumber = TextField('ticketnumber', validators = [Required(), Regexp('^[0-9]*$', flags = 0, message = u'Field must be an integer')])