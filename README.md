# ETLPostgresToRedshift
Extração, Tratamento e Inserção de tabelas de um Database POSTGRES para o REDSHIFT.

# Necessário:
Necessário criar um arquivo '.env' na raiz do projeto e colocar as credenciais dos bancos e tbm da conta na AWS.

# Como funciona:
- Vc informa um schema e uma tabela que quer quer copiar para o Redshift.
- O script faz o export dessa tabela e salva num csv, dentro da pasta 'csv_files'.
- Faz contagem de registros exportados e salva.
- Split o csv em arquivos de 50 ou 100mil linhas (vc pode alterar isso para o que achar melhor.)
- Cria uma tabela temporaria onde vamos inserir os dados desse Export.
- Itera sobre todos os arquivos da pasta 'csv_files/split_files' e arquivo por arquivo els faz:
  * Upload do arquivo para um Bucket S3.
    - Se o UPLOAD pro S3 foi sucess:
      * Faz o COPY/INSERT para a tabela temporária que criamos no REDSHIFT.
      * Apaga o arquivo tanto da pasta csv_files quanto do bucket S3.
- Após fazer TODOS os INSERTs faz uma verificação se a quantidade de registros inseridos é igual a exportada.
  * Se verdadeira a condição acima ele renomeia a tabela temporária e apaga a temporaria.


# VERIFICAR ERROR REDSHIFT:
Podem acontecer erros em uma ou outra tabela no momento da inserção, devido a tipagem incorreta. O script conta com um dicionário que fiz onde podemos forcar uma tipagem somente inseriondo o nome da tabela e coluna.
Vc pode consultar que erros aconteceram e em quais tabelas e colunas no REDSHIFT com o comando abaixo:
'SELECT * FROM stl_load_errors ORDER BY starttime DESC;'
