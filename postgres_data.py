from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum
import psycopg2
from django.core.serializers.json import DjangoJSONEncoder
import json


local_tz = pendulum.timezone("Europe/Warsaw")
conn_source = psycopg2.connect(host="localhost", database="timeregistry", user="<username>",
                        password="<password>")

conn_destination = psycopg2.connect(host="localhost", database="postgres", user="<username>", password="<password>")


file_path = '/usr/local/airflow/dags/results.json'


def get_fresh_data():
    columns = ('id', 'card', 'TimeStampIn', 'TimeStampOut')
    results = []
    open(file_path, "w").write("")
    open(file_path).close()

    try:
        cur = conn_source.cursor()
        sql = """select id, card, "TimeStampIn", "TimeStampOut" from register_timeregister where uploaded = false"""
        cur.execute(sql)
        for row in cur.fetchall():
            results.append(dict(zip(columns, row)))
        handle = open(file_path, 'a')
        handle.write(json.dumps(results, indent=2, cls=DjangoJSONEncoder))
        handle.close()
        conn_source.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn_source is not None:
            conn_source.close()


def insert_data():
    cur = conn_destination.cursor()
    cur_source = conn_source.cursor()
    with open(file_path) as f:
        d = json.load(f)
    sql_new = """insert into hr_working_hours_workinghours(source_id, card, entry_time, leaving_time, accepted, holiday)
                        values(%s, %s, %s, %s, %s, %s) returning card;"""
    try:
        for n in d:
            source_id = str(list(n.values())[0])
            card = str(list(n.values())[1])
            entry_time = str(list(n.values())[2])
            leaving_time = str(list(n.values())[3])
            accepted = False
            holiday = False
            cur.execute(
                "select exists (select source_id from hr_working_hours_workinghours where source_id = {0})"
                    .format(source_id),
                ("source_id", source_id)
            )
            conn_destination.commit()
            answer = cur.fetchone()
            a = str(answer[0])
            print(source_id)
            if a == 'False':
                if leaving_time == 'None':
                    cur.execute(sql_new, (
                        source_id, card, entry_time, None, accepted, holiday))
                    conn_destination.commit()

                if leaving_time != 'None':
                    cur.execute(sql_new, (
                        source_id, card, entry_time, leaving_time, accepted, holiday))
                    conn_destination.commit()
            if a == 'True':
                if leaving_time != 'None':
                    cur.execute(
                        "update hr_working_hours_workinghours set leaving_time = '{1}' where source_id = {0}"
                            .format(source_id, leaving_time),
                        (("source_id", source_id), ('leaving_time', leaving_time)))
                    conn_destination.commit()
                    cur_source.execute(
                        "update register_timeregister set uploaded = true where id = '{0}' and 'TimeStampOut' is not NULL".format(
                            source_id),
                        ("source_id", source_id))
                    conn_source.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn_destination is not None:
            conn_destination.close()
    cur.close()


def mark_source_uploaded():
    with open(file_path) as f:
        d = json.load(f)
    cur = conn_source.cursor()
    for n in d:
        id = str(list(n.values())[0])
        print(id)
        cur.execute("""update register_timeregister set uploaded = true where id = '{0}' and "TimeStampIn" <= NOW() - INTERVAL '12 hour'""".format(id),
                    ("id", id))
        conn_source.commit()
    cur.close()


def update_kzz():
    cur = conn_destination.cursor()
    sql = """select hr_working_hours_workinghours.card, auth_user.username
                from hr_working_hours_workinghours inner join stolowka_usercompanycard on hr_working_hours_workinghours.card = stolowka_usercompanycard.card
                inner join auth_user on stolowka_usercompanycard.user_id = auth_user.id
                where hr_working_hours_workinghours.kzz is Null"""
    cur.execute(sql)
    for row in cur.fetchall():
        card = row[0]
        kzz = row[1]
        cur.execute("update hr_working_hours_workinghours set kzz = '{0}' where kzz is Null and card = '{1}'"
                    .format(kzz, card),
                    (("kzz", kzz), ("card", card)))
        conn_destination.commit()
    cur.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 8, 27, tzinfo=local_tz),
    'retries': 2,
}

dag = DAG(
    'postgres_data',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    max_active_runs=1
    )

get_fresh_data_operator = PythonOperator(
    task_id='get_fresh_data',
    python_callable=get_fresh_data,
    dag=dag
    )

postgres_operator = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag
    )

mark_source_uploaded_operator = PythonOperator(
    task_id='mark_source_uploaded',
    python_callable=mark_source_uploaded,
    dag=dag
    )

update_kzz = PythonOperator(
    task_id='update_kzz',
    python_callable=update_kzz,
    dag=dag
    )

get_fresh_data_operator >> postgres_operator >> mark_source_uploaded_operator >> update_kzz
