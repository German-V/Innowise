import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.decorators import task_group
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import MongoClient
import pymongo
# from airflow.operators.dummy import DummyOperator


import os
import pandas as pd
import shutil

main_path = f"{os.getcwd()}"
# shutil.copy(main_path+"/files/tiktok_google_play_reviews.csv",main_path+"/dataset_data/tiktok_google_play_reviews.csv")
ds = Dataset(main_path+"/files/tiktok_google_play_reviews.csv")


def is_not_empty():
    if os.stat(main_path+"/files/tiktok_google_play_reviews.csv").st_size != 0:
        return "not_empty"
    else:
        return "empty_file"
        
def edit_null_fun(**kwargs):
    ti = kwargs["ti"]
    # df = ti.xcom_pull(task_ids="check-status", key="signal")
    df = kwargs['df']
    df = df.fillna('-')
    
    ti.xcom_push("df", df)
        
def sort_fun(**kwargs):
    ti = kwargs["ti"]
    df = ti.xcom_pull(task_ids="data_manipulation.edit_null", key="df")
    # df = kwargs['df']
    df = df.sort_values(by=['at'])
    # ti = kwargs["ti"]
    ti.xcom_push("df", df)
    
def apply_remove_emojis(cell):
    if isinstance(cell, float):
        return str(cell)
    return cell.encode('ascii', 'ignore').decode('utf-8')

def remove_emojis(**kwargs):
    ti = kwargs["ti"]
    df = ti.xcom_pull(task_ids="data_manipulation.sort", key="df")
    # df = kwargs['df']
    df['content'] = df['content'].apply(apply_remove_emojis)
    df.to_csv("result.csv")
    # ti = kwargs["ti"]
    ti.xcom_push("df", df)
    
        
path = f"{os.getcwd()}/files/tiktok_google_play_reviews.csv"
with DAG(dag_id="task_7",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    schedule="@once") as dag:
    
    check_file = FileSensor(task_id="wait_for_file", filepath=path),
    
    branching = BranchPythonOperator(task_id='branching', python_callable=is_not_empty)
    
    empty_file = BashOperator(task_id = "empty_file", bash_command='echo "File is empty!";')
    
    not_empty = EmptyOperator(task_id = 'not_empty')
    
    data = pd.read_csv(path)
    data.head()

    check_file>>branching>>Label("empty_file")>>empty_file

    @task_group()
    def data_manipulation():
        edit_null = PythonOperator(task_id="edit_null", python_callable=edit_null_fun, op_kwargs={"df": data})
        sort = PythonOperator(task_id="sort", python_callable=sort_fun, op_kwargs={"df": data})
        remove = PythonOperator(task_id="remove", python_callable=remove_emojis, op_kwargs={"df": data}, outlets = [ds])
        edit_null>>sort>>remove
    check_file>>branching>>Label("not_empty")>>not_empty>>data_manipulation()
    
with DAG(dag_id="task_7_mongoDB",
    start_date=datetime.datetime(2024, 1, 1),
    schedule=[ds],
    catchup=False) as dag:
    
    def conn_and_insert():
        data = pd.read_csv("result.csv")
        data.head()
        # hook = MongoHook(mongo_conn_id='mongo_db')
        # client = hook.get_conn()
        client = MongoClient("mongodb://admin:password@localhost:27017/")
        db = client['taskDB']
        collection = db['reviews']
        collection.insert_many(data.to_dict(orient='records'))
    
    insert_mongodb = PythonOperator(task_id='insert_mongodb', python_callable=conn_and_insert)
    insert_mongodb
    
    # Топ-5 часто встречаемых комментариев
    # db.reviews.aggregate([{ $group: { _id: "$content", count: { $sum: 1 } } }, { $sort: { count: -1 } }, { $limit: 5 }]);

    # Все записи, где длина поля “content” составляет менее 5 символов
    # db.reviews.find({"$expr": {"$lt": [{"$strLenCP": "$content"}, 5]}})
    
    # Средний рейтинг по каждому дню
    # db.reviews.aggregate([{"$group": {_id: {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}}, "avg_rating": {"$avg": "$rating"}}}, {"$project": {"_id": 0, "date": "$_id", "avg_rating": 1}}, {"$sort": {"date": 1}}])
