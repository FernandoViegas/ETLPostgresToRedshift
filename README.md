# ETLPostgresToRedshift
Extração, Tratamento e Inserção de tabelas de um Database POSTGRES para o REDSHIFT.

Necessário criar um arquivo '.env' na raiz do projeto e colocar as credenciais dos bancos e tbm da conta na AWS.
Informções ṕara colocar no arquivo '.env' :
  # CREDENCIALS POSTGRES 
  PGHOST='host'
  PGPORT='port'
  PGDATABASE='database'
  PGUSER='user'
  PGPASSWORD='xxxxxxxxxxxxxxxxxx'

  # CREDENCIALS REDSHIFT

  RDHOST='host'
  RDPORT='port'
  RDDATABASE='database'
  RDUSER='user'
  RDPASSWORD='xxxxxxxxxxxxxxxxxxxxxxx'

  # CREDENCIALS AWS
  AWS_ACCESS_KEY_ID='xxxxxxxxxxxxxxxxxxxxxxxxxx' 
  AWS_SECRET_ACCESS_KEY='xxxxxxxxxxxxxxxxxxxxxxxxx'
  BUCKET_NAME='xxxxx-xxxxxx-xxxxxxx' 
  FOLDER_ROUTE='folder_name/' 
