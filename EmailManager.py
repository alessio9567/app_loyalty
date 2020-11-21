import smtplib
import CommonUtility
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from os.path import basename
from email.mime.application import MIMEApplication
from email import encoders
from email.mime.base import MIMEBase
import Logger


SMTP_IP = CommonUtility.getParameterFromFile('SMTP_IP')
EMAIL_TO = CommonUtility.getParameterFromFile('EMAIL_TO')
EMAIL_FROM = CommonUtility.getParameterFromFile('EMAIL_FROM')

email_to_list = EMAIL_TO.split(', ')

def sendSimpleEmail(email_title, email_body):
    mail = smtplib.SMTP(SMTP_IP)
    subject = "Loyalty FcaBank " + email_title
    body = "<html><body>"
    body += email_body
    body += "</body></html>"
    ###
    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    #msg['To'] = ', '.join(EMAIL_TO)
    msg['To'] = EMAIL_TO
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))
    ###
    mail.sendmail(EMAIL_FROM, email_to_list, msg.as_string())

def parse_log(file_name, body):
    with open(file_name) as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    errors = [line for line in content if '- ERROR -' in line or
              '- WARNING -' in line]
    body += '<b>Warning o errori:</b><br><font color="red">'
    body += '<br>'.join(errors)
    body += '</font>'
    return body

def sendEmailWithAttachment(email_title, email_body, filepath=""):
    if filepath == "":
        logger = Logger.getLogger()
        filepath = logger.logpath
    mail = smtplib.SMTP(SMTP_IP)
    subject = "Loyalty FcaBank " + email_title
    body = "<html><body>"
    body += email_body
    errori = parse_log(filepath, body)
    body += errori
    body += "</body></html>"
    ###
    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    # msg['To'] = ', '.join(EMAIL_TO)
    msg['To'] = EMAIL_TO
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))
    ###
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(filepath, "rb").read())
    filename = filepath[filepath.rfind('/') + 1:]
    attachment_parameter = 'attachment; filename={}'.format(filename)
    part.add_header('Content-Disposition', attachment_parameter)
    msg.attach(part)
    ###
    mail.sendmail(EMAIL_FROM, email_to_list, msg.as_string())
