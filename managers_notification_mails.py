from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pendulum
import psycopg2
import datetime

local_tz = pendulum.timezone("Europe/Warsaw")
conn_source = psycopg2.connect(host="<hostname>", database="w16v5", user="<username>", password="<password>")

#GMAIL
EMAIL_HOST = 'smtp.gmail.com'
EMAIL_USE_TLS = True
EMAIL_PORT = 587
EMAIL_HOST_USER = ''
EMAIL_HOST_PASSWORD = ''


def get_managers():
    global emails
    emails = []
    cur = conn_source.cursor()
    get_managers = """select distinct hr_working_hours_locationtomanager.manager_id, auth_user.email
                        from hr_working_hours_locationtomanager
                        inner join auth_user on hr_working_hours_locationtomanager.manager_id = auth_user.id"""
    cur.execute(get_managers)
    for n in cur.fetchall():
        if n[1] != '':
            emails.append(n[1])


def send_mails():
    email_sender = EMAIL_HOST_USER
    email_to = emails
    email_bcc = ['zmua@viessmann.com', 'malt@viessmann.com', 'grkk@viessmann.com']
    subject = 'Przypomnienie: proszę zaktualizować grafik godzin pracy.'

    msg = MIMEMultipart()
    msg['From'] = email_sender
    msg['To'] = ",".join(email_to)
    msg['Bcc'] = ",".join(email_bcc)
    msg['Subject'] = subject

    body = 'Wiadomość została wygenerowana automatycznie. Proszę na nią nie odpowiadać.'
    msg.attach(MIMEText(body, 'plain'))

    text = msg.as_string()

    email_send = email_to + email_bcc

    connection = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    connection.starttls()
    connection.login(email_sender, EMAIL_HOST_PASSWORD)
    connection.sendmail(email_sender, email_send, text)
    connection.quit()


default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 1, 20, tzinfo=local_tz),
    'retries': 2,
}

dag = DAG(
    'managers_notification_mails', default_args=default_args, schedule_interval='0 1 26 * *')

get_managers = PythonOperator(
    task_id='get_managers', python_callable=get_managers, dag=dag)

send_mails = PythonOperator(
    task_id='send_mails', python_callable=send_mails, dag=dag)

get_managers >> send_mails
