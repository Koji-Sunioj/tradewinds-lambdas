import os
import boto3
import base64
import psycopg2
import psycopg2.extras

db_params = os.environ.get("DB_ADMIN")
encoded = db_params.encode("utf-8")
db_name,user,password,port = base64.b64decode(encoded).decode("utf-8").split(",")

connect_pooler = "postgres://%s.ljqjiwbxuqrihfepogdg:%s@aws-1-eu-north-1.pooler.supabase.com:%s/%s" % (user,password,port,db_name)
conn = psycopg2.connect(connect_pooler)

def handler(event, context):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("select * from customers;")
    print(cursor.fetchall())

