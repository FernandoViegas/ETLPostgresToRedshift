import os
import pandas as pd
import boto3
import logging
import time
import psycopg2
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import date

load_dotenv()
# DB Postgres - SOURCE
host_postgres = os.environ.get('PGHOST')
port_postgres = os.environ.get('PGPORT')
db_postgres = os.environ.get('PGDATABASE')
user_postgres = os.environ.get('PGUSER')
pwd_postgres = os.environ.get('PGPASSWORD')
# DB Redshift - DESTINATION
host_redshift = os.environ.get('RDHOST')
port_redshift = os.environ.get('RDPORT')
db_redshift = os.environ.get('RDDATABASE')
user_redshift = os.environ.get('RDUSER')
pwd_redshift = os.environ.get('RDPASSWORD')
# CREDENTIALS AWS
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
s3_bucket_name = os.environ.get('BUCKET_NAME')
s3_folder_route = os.environ.get('FOLDER_ROUTE')

def main(schema_source, table_name):
  date_now = str(date.today())
  csv_name = f'temp_{table_name}_{date_now}.csv'
  csv_path = f'./csv_files/{csv_name}'
  # para rodar pelo AIRFLOW é necessário 
  # csv_path = f'/opt/airflow/folder_project/csv_files/{csv_name}' 
  extract_status = extract_dq(schema_source, table_name, csv_path)
  if extract_status:
    count_rows_exported = count_source(table_name, schema_source)
    print(count_rows_exported)    
    schema_destination = dw_schema_name(table_name)
    split_files_path = split_csv(csv_name, table_name, count_rows_exported)
    status_create = create_cmd_sql(table_name, schema_destination, csv_path, csv_name)
    print('STATUS CREATE TEMP_TABLE: ', status_create)
    if status_create:
      files = os.listdir(split_files_path)
      print(f'CSV SPLITADO EM: {len(files)} files.')
      count = 1
      for file in files:
        print("FILE: ", count)
        if file.endswith(('_000')):
          try:
            os.system(f"cd {split_files_path} && tail -n +2 {file} > tmp.csv && mv tmp.csv {file}") # removendo a primeira linha do arquivo que termina com _000, que contem as colunas.
          except Exception as e:
            print('ERROR REMOVE FIRST LINE file_000:', table_name )
            print(e)
        try:    
          os.system(f"cd '{split_files_path}' && mv '{file}' '{file}.csv'") # renomeando files com '.csv'
        except Exception as e:
          print('ERROR IN RENAME FILE TO .CSV', table_name)
          print(e)
        csv_path = f"{split_files_path}/{file}.csv"
        upload = upload_to_s3(csv_path, csv_name)
        if upload:
          status_copy = copy_to_redshift(schema_destination, table_name, csv_name)
          print('Status Copy: ', status_copy)
          delete_csv(csv_name, csv_path)
          # print('Status Delete: ', status_delete)
        count += 1
    else:
      print('PROBLEMAS COM CREATE AND DROP TEMP_TABLE', table_name)
  else:
    print('PROBLEMAS AO EXPORTAR REGISTROS DA TABELA: ', table_name)
    pass
  status = replace_temp_table(schema_destination, table_name, count_rows_exported)
  return status
  
def extract_dq(schema_source, table_name, csv_path):
  print(f'EXPORTANDO DADOS DA TABELA {schema_source}.{table_name}')
  cmd_copy = f"PGPASSWORD='{pwd_postgres}' psql -h {host_postgres} -d {db_postgres} -U {user_postgres} -c \"\COPY (SELECT * FROM {schema_source}.{table_name} ) TO '{csv_path}' WITH DELIMITER ',' CSV HEADER;\""
  try:
    os.system(cmd_copy)
    return True
  except Exception as e:
    print(e)
    return False

def count_source(table_name, schema_source):
  conn = psycopg2.connect(host=host_postgres, port=port_postgres, database=db_postgres, user=user_postgres, password=pwd_postgres)
  cur = conn.cursor()
  try:
    cur.execute(f'SELECT COUNT(1) FROM {schema_source}.{table_name};')
    result=cur.fetchone()
    return result[0]
  except Exception as e:
    print(e)

def dw_schema_name(table_name):
  dict_schema_redshift = {'tabela_name1': 'source_redshift1',
                    'tabela_name2': 'source_redshift2',
                    'tabela_name3': 'source_redshift3',
                    'tabela_name4': 'source_redshift4',
                    'tabela_name5': 'source_redshift5',
                    'tabela_name6': 'source_redshift6'
  }
  if dict_schema_redshift[table_name]:
    schema_name = dict_schema_redshift[table_name]
    print('SCHEMA DW: ', schema_name)
    return schema_name
  else:
    print('TABELA NÃO IDENTIFICADA PARA O CORRETO SCHEMA REDSHIFT: ', table_name)

def split_csv(csv_name, table_name, count_rows_exported):
  print('SPLITANDO CSV')
  # Rever essa questão de splitar em 100mil ou 50 mil linhas. Pq não 50mil pra todos??
  lines = '100000' if count_rows_exported > 1000000 else '50000' 
  folder_path = './csv_files/split_files'
  # para rodar pelo AIRFLOW é necessário 
  # folder_path = '/opt/airflow/folder_project/csv_files/split_files' 
  cmd_split = f"cd {folder_path} && split --numeric-suffixes --suffix-length=3 --lines={lines} '../{csv_name}' {table_name}_"
  try:
    os.system(cmd_split)
    return folder_path
  except Exception as e:
    print('ERROR IN SPLIT: ', table_name)
    print(e)
    
def create_cmd_sql(table_name, schema_destination, csv_path, csv_name):
  print('CREATE COMMAND SQL')
  create_sql = f"DROP TABLE IF EXISTS {schema_destination}.temp_{table_name};\nCREATE TABLE IF NOT EXISTS {schema_destination}.temp_{table_name} ("
  df = pd.read_csv(csv_path, engine='python', nrows=1000000)
  df = df.convert_dtypes()
  pd_types = df.dtypes
  columns = df.columns
  len_columns = len(columns)
  i = 0
  force_varchar_max = {'table_name1': ['columnA', 'columnB', 'columnC'],
                      'table_name2': ['columnZ'],
                      'table_name3': ['columnX'],
                      'table_name4': ['columnE', 'columnF', 'columnG']
                      }
  force_timestamp = {}
  force_float = {'table_name2': ['columnA']}
  while i < len_columns:
    column = "\""+str(columns[i])+"\""
    pd_type = str(pd_types[i])
    if column.endswith('_at\"') or column.endswith('_date\"'):
      type = 'TIMESTAMP'
    elif column.endswith('_fees\"'):
      type = 'FLOAT'
    # elif column.endswith('_id\"'):
    #   type = 'VARCHAR'
    elif table_name in force_varchar_max and str(columns[i]) in force_varchar_max[table_name]:
      type = 'VARCHAR(MAX)'
    elif table_name in force_timestamp and str(columns[i]) in force_timestamp[table_name]:
      type = 'TIMESTAMP'
    elif table_name in force_float and str(columns[i]) in force_float[table_name]:
      type = 'FLOAT'
    else:
      type = pd_dtype_to_redshift_dtype(pd_type)
    create_sql = create_sql + column + " " + type
    if (i+1) < len_columns:
      create_sql = create_sql + ', '
    else:
      create_sql = create_sql + ');'
    i += 1
  print(create_sql)
  status = exec_create_table(create_sql, schema_destination, table_name, csv_name)
  return(status)

def pd_dtype_to_redshift_dtype(dtype):
  if dtype.startswith('int64') or dtype.startswith('Int64'):
    return 'BIGINT'
  elif dtype.startswith('int'):
    return 'INTEGER'
  elif dtype.startswith('float') or dtype.startswith('Float'):
    return 'FLOAT'
  elif dtype.startswith('date'):
    return 'TIMESTAMP'
  elif dtype == 'bool':
    return 'BOOLEAN'
  else:
    return 'VARCHAR'

def exec_create_table(create_sql, schema, table_name, csv_name):
  print('EXECUTANDO CREATE TABLE')
  conn = psycopg2.connect(host=host_redshift, port=port_redshift, database=db_redshift, user=user_redshift,password=pwd_redshift)
  cur = conn.cursor()
  cur.execute(create_sql)
  conn.commit()
  cur.execute(f'SELECT count(1) from {schema}.temp_{table_name};')
  result = cur.fetchone()
  if result[0] == 0:
    try:
      os.system(f"cd './csv_files/' && rm '{csv_name}'")
      # para rodar pelo AIRFLOW é necessário 
      # os.system(f"cd '/opt/airflow/folder_project/csv_files/' && rm '{csv_name}'")
      return True
    except Exception as e:
      print('ERROR IN EXEC CREATE TABLE: ', table_name)
      print(e)
      return False
  else:
    return False

def upload_to_s3(csv_path, csv_name):
  # print('UPLOAD TO S3')
  session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
  )
  s3 = session.client('s3')
  with open(csv_path, "rb") as f:
    try: 
      s3.upload_fileobj(f, s3_bucket_name, s3_folder_route+csv_name)
      return True
    except ClientError as e:
      logging.error(e)
      print(e)
      return False
    
def copy_to_redshift(schema_destination, table_name, csv_name):
  # print("COPY to Redshift")
  conn = psycopg2.connect(host=host_redshift, port=port_redshift, database=db_redshift, user=user_redshift,password=pwd_redshift)
  cur = conn.cursor()

  cmd_insert = f"""COPY {schema_destination}.temp_{table_name} 
  FROM 's3://{s3_bucket_name}/{s3_folder_route}{csv_name}'
  credentials 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}'
  DELIMITER ',' CSV NULL 'NaN';
  """ # IGNOREHEADER 1 não é necessario pq eu removo as colunas no primeiro csv splitado.
  try:
    cur.execute(cmd_insert)
    conn.commit()
    return True
  except Exception as e:
    print('ERROR in COPY TO REDSHIFT: ', table_name)
    print(e)
    return False

def delete_csv(csv_name, csv_path):
  # print('Deleting file CSV in ', csv_path)
  try:
    os.system('rm '+csv_path)
  except ClientError as e:
    print('ERROR IN Deleting file CSV', csv_path)
    print(e)

  # print("Deleting file CSV in bucket S3")
  session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
  )
  s3 = session.resource('s3')
  try: 
    s3.Object(s3_bucket_name, s3_folder_route+csv_name).delete()
  except ClientError as e:
    logging.error(e)
    print('ERROR Deleting file in BUCKET S3', s3_folder_route+csv_name)
    print(e)

def replace_temp_table(schema_destination, table_name, count_rows_exported):
  # print('REPLACE temp_table')
  conn = psycopg2.connect(host=host_redshift, port=port_redshift, database=db_redshift, user=user_redshift,password=pwd_redshift)
  cur = conn.cursor()
  count_cmd = f"select count(1) from {schema_destination}.temp_{table_name}"
  cur.execute(count_cmd)
  result = cur.fetchone()
  count_row_inserted = result[0]
  if count_row_inserted == count_rows_exported:
    print('COUNT ROWS INSERTED *IGUAL* COUNT ROWS EXPORTED')
    # DROP_TABLE
    drop_cmd = f"DROP TABLE IF EXISTS {schema_destination}.{table_name};"
    cur.execute(drop_cmd)
    conn.commit()
    time.sleep(5)
    # RENAME_TABLE
    rename_cmd = f"ALTER TABLE {schema_destination}.temp_{table_name} RENAME TO {table_name};"
    cur.execute(rename_cmd)
    conn.commit()
    conn.close()
    return True
  elif count_row_inserted < count_rows_exported:
    print('COUNT ROWS INSERTED ** MENOR QUE ** COUNT ROWS EXPORTED')
    print('COUNT ROWS EXPORTED: ', count_rows_exported)
    print('COUNT ROWS INSERTED: ', count_row_inserted)
    return False
    # inserir alguma notificação via Slack !!!
  elif count_row_inserted > count_rows_exported:
    print('COUNT ROWS INSERTED ** MAIOR QUE ** COUNT ROWS EXPORTED')
    print('COUNT ROWS EXPORTED: ', count_rows_exported)
    print('COUNT ROWS INSERTED: ', count_row_inserted)
    return False
    # inserir alguma notificação via Slack !!!

main(schema_source='schema', table_name='table_name')