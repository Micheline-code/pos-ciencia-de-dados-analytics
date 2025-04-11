# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **1-Engenharia-de-Dados-MVP-Coleta-Criação-Carga-Qualidade-Bronze**
# MAGIC
# MAGIC Este notebook faz coleta e ingestão dos arquivos "Regulacao_Escolas_2024.csv" e "Regulacao_Cursos_2024.csv", disponibilizados na plataforma através da opção "+ New" -> "Add or upload". Os dados serão gravados nas tabelas "escolas_sistec" e "cursos_sistec" da camada bronze. Por fim, a qualidade dos dados é verificada: listagem de campos e tipos correspondentes; listagem de domínios de campos categóricos; contagem de registros (total e distintos); contagem de ocorrências de campos nulos; contagem de ocorrências de tipos incompatíveis (letras em campos numérios).
# MAGIC
# MAGIC Os dados de escolas e cursos foram extraídos do **Portal Brasileiro de Dados Abertos** e **Catálogo Nacional de Dados**.
# MAGIC
# MAGIC **Educação profissional e tecnológica (SISTEC) - Regulação Escolas**
# MAGIC https://dados.gov.br/dados/conjuntos-dados/educacao-profissional-e-tecnologica--sistec---regulacao-escolas
# MAGIC
# MAGIC **Educação profissional e tecnológica (SISTEC) - Regulação Cursos**
# MAGIC https://dados.gov.br/dados/conjuntos-dados/educacao-profissional-e-tecnologica--sistec---regulacao-cursos
# MAGIC

# COMMAND ----------

# DBTITLE 1,Ingestão de dados de escolas
# 2) Ingestão de dados de escolas

# Local e tipo do arquivo
file_location = "/FileStore/tables/meu_mvp_1/Regulacao_Escolas_2024.csv"
file_type = "csv"

# Opções csv
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# Leitura do arquivo de escolas
df_escolas_spark = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_escolas_spark)

# COMMAND ----------

# DBTITLE 1,Ingestão de dados de cursos
# 3) Ingestão de dados de cursos

# Local e tipo do arquivo
file_location = "/FileStore/tables/meu_mvp_1/Regulacao_Cursos_2024.csv"
file_type = "csv"

# Opções csv
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# Leitura do arquivo de cursos
df_cursos_spark = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df_cursos_spark)

# COMMAND ----------

# DBTITLE 1,Criando camada bronze
# MAGIC %sql
# MAGIC
# MAGIC -- 4) Criando camada bronze
# MAGIC
# MAGIC -- Criando camada bronze
# MAGIC CREATE DATABASE bronze
# MAGIC

# COMMAND ----------

# DBTITLE 1,Salvando tabelas na camada bronze
# 5) Salvando tabelas na camada bronze

# Salvando tabela escolas
df_escolas_spark.write.mode("overwrite").saveAsTable("bronze.escolas_sistec")

# Salvando tabela cursos
df_cursos_spark.write.mode("overwrite").saveAsTable("bronze.cursos_sistec")

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: escolas_sistec - esquema
# 6) Verificando qualidade de dados: escolas_sistec - esquema

# Listando esquema da tabela escolas_sistec

df_escolas_spark.printSchema

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: escolas_sistec - quantidade de escolas
# MAGIC %sql
# MAGIC
# MAGIC -- 7) Verificando qualidade de dados: escolas_sistec - quantidade de escolas
# MAGIC
# MAGIC -- Listando quantidade de escolas
# MAGIC
# MAGIC SELECT COUNT(CODIGO_UNIDADE_DE_ENSINO) FROM bronze.escolas_sistec
# MAGIC
# MAGIC -- Resultado: quantidade de escolas = 22.508

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: escolas_sistec - quantidade distinta de escolas
# MAGIC %sql
# MAGIC
# MAGIC -- 8) Verificando qualidade de dados: escolas_sistec - quantidade distinta de escolas
# MAGIC
# MAGIC -- Listando quantidade distinta de escolas
# MAGIC
# MAGIC SELECT COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) FROM bronze.escolas_sistec
# MAGIC
# MAGIC -- Resultado: quantidade distinta de escolas = 22.508

# COMMAND ----------

# DBTITLE 1,Verificando qualidade dos dados - escolas_sistec - nulos
# MAGIC %sql 
# MAGIC
# MAGIC -- 9) Verificando qualidade dos dados - escolas_sistec - nulos
# MAGIC
# MAGIC -- Verificando quantidade de ocorrências de nulos por campos
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(CASE WHEN NOME_SISTEMA_DE_ENSINO IS NULL THEN 1 END) AS NOME_SISTEMA_DE_ENSINO_nulos,
# MAGIC     COUNT(CASE WHEN NOME_TIPO_ESCOLA IS NULL THEN 1 END) AS NOME_TIPO_ESCOLA_nulos,
# MAGIC     COUNT(CASE WHEN CODIGO_UNIDADE_DE_ENSINO IS NULL THEN 1 END) AS CODIGO_UNIDADE_DE_ENSINO_nulos,
# MAGIC     COUNT(CASE WHEN UNIDADE_DE_ENSINO IS NULL THEN 1 END) AS UNIDADE_DE_ENSINO_nulos,
# MAGIC     COUNT(CASE WHEN NOME_DEPENDENCIA_ADMINISTRATIVA IS NULL THEN 1 END) AS NOME_DEPENDENCIA_ADMINISTRATIVA_nulos,
# MAGIC     COUNT(CASE WHEN NOME_SUBDEPENDENCIA_ADMINISTRATIVA IS NULL THEN 1 END) AS NOME_SUBDEPENDENCIA_ADMINISTRATIVA_nulos,
# MAGIC     COUNT(CASE WHEN UF IS NULL THEN 1 END) AS UF_nulos,
# MAGIC     COUNT(CASE WHEN CODIGO_MUNICIPIO IS NULL THEN 1 END) AS CODIGO_MUNICIPIO_nulos,
# MAGIC     COUNT(CASE WHEN MUNICIPIO IS NULL THEN 1 END) AS MUNICIPIO_nulos,
# MAGIC     COUNT(CASE WHEN CEP IS NULL THEN 1 END) AS CEP_nulos,
# MAGIC     COUNT(CASE WHEN LOGRADOURO IS NULL THEN 1 END) AS LOGRADOURO_nulos,
# MAGIC     COUNT(CASE WHEN NUMERO IS NULL THEN 1 END) AS NUMERO_nulos,
# MAGIC     COUNT(CASE WHEN E_MAIL IS NULL THEN 1 END) AS E_MAIL_nulos,
# MAGIC     COUNT(CASE WHEN TELEFONE_1 IS NULL THEN 1 END) AS TELEFONE_1_nulos,
# MAGIC     COUNT(CASE WHEN TELEFONE_2 IS NULL THEN 1 END) AS TELEFONE_2_nulos,
# MAGIC     COUNT(CASE WHEN CODIGO_EMEC_ENDERECO_CAMPUS IS NULL THEN 1 END) AS CODIGO_EMEC_ENDERECO_CAMPUS_nulos,
# MAGIC     COUNT(CASE WHEN CODIGO_IES IS NULL THEN 1 END) AS CODIGO_IES_nulos,
# MAGIC     COUNT(CASE WHEN CODIGO_INEP IS NULL THEN 1 END) AS CODIGO_INEP_nulos
# MAGIC FROM bronze.escolas_sistec
# MAGIC
# MAGIC -- Resultado: colunas com valores nulos (quantidade entre parêntesis) = CEP (4), LOGRADOURO (11), NUMERO (18), E_MAIL (23), TELEFONE_1 (20), TELEFONE_2 (12.364)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: escolas_sistec - domínios
# MAGIC %sql
# MAGIC
# MAGIC -- 10) Verificando qualidade de dados: escolas_sistec - domínios
# MAGIC
# MAGIC -- Todas as consultas para listar domínios dos campos de interesse do estudo na tabela escolas_sistec estão nessa célula; deve-se retirar os caracteres de comentários quando for executar cada comando específico
# MAGIC
# MAGIC --SELECT DISTINCT NOME_SISTEMA_DE_ENSINO FROM bronze.escolas_sistec
# MAGIC --Resultado:
# MAGIC --ESTADUAL / DISTRITAL
# MAGIC --MUNICIPAL
# MAGIC --FEDERAL
# MAGIC
# MAGIC --SELECT DISTINCT NOME_TIPO_ESCOLA FROM bronze.escolas_sistec
# MAGIC --Resultado:
# MAGIC --Escolas de Oferta Exclusiva FIC
# MAGIC --Escolas de Oferta Outros Cursos
# MAGIC --IPES
# MAGIC --Escolas Técnicas
# MAGIC
# MAGIC --SELECT DISTINCT NOME_DEPENDENCIA_ADMINISTRATIVA FROM bronze.escolas_sistec
# MAGIC --Resultado:
# MAGIC --PÚBLICA
# MAGIC --PRIVADA
# MAGIC --MILITAR
# MAGIC --ARTIGO 240 - SISTEMA S
# MAGIC
# MAGIC --SELECT DISTINCT NOME_SUBDEPENDENCIA_ADMINISTRATIVA FROM bronze.escolas_sistec
# MAGIC --Resultado:
# MAGIC --SENAR
# MAGIC --Marinha
# MAGIC --SESI
# MAGIC --SENAT
# MAGIC --SESCOOP
# MAGIC --SENAI
# MAGIC --Aeronáutica
# MAGIC --SENAC
# MAGIC --null
# MAGIC --SESC
# MAGIC
# MAGIC SELECT NOME_SISTEMA_DE_ENSINO, NOME_TIPO_ESCOLA,NOME_DEPENDENCIA_ADMINISTRATIVA, NOME_SUBDEPENDENCIA_ADMINISTRATIVA FROM bronze.escolas_sistec GROUP BY NOME_SISTEMA_DE_ENSINO, NOME_TIPO_ESCOLA,NOME_DEPENDENCIA_ADMINISTRATIVA, NOME_SUBDEPENDENCIA_ADMINISTRATIVA ORDER BY NOME_SISTEMA_DE_ENSINO, NOME_TIPO_ESCOLA,NOME_DEPENDENCIA_ADMINISTRATIVA, NOME_SUBDEPENDENCIA_ADMINISTRATIVA
# MAGIC
# MAGIC --Resultado:
# MAGIC --NOME_SISTEMA_DE_ENSINO	NOME_TIPO_ESCOLA	NOME_DEPENDENCIA_ADMINISTRATIVA	NOME_SUBDEPENDENCIA_ADMINISTRATIVA
# MAGIC --ESTADUAL / DISTRITAL	Escolas Técnicas	PRIVADA	null
# MAGIC --ESTADUAL / DISTRITAL	Escolas Técnicas	PÚBLICA	null
# MAGIC --ESTADUAL / DISTRITAL	Escolas de Oferta Exclusiva FIC	PRIVADA	null
# MAGIC --ESTADUAL / DISTRITAL	Escolas de Oferta Exclusiva FIC	PÚBLICA	null
# MAGIC --FEDERAL	Escolas Técnicas	ARTIGO 240 - SISTEMA S	SENAC
# MAGIC --FEDERAL	Escolas Técnicas	ARTIGO 240 - SISTEMA S	SENAI
# MAGIC --FEDERAL	Escolas Técnicas	ARTIGO 240 - SISTEMA S	SENAR
# MAGIC --FEDERAL	Escolas Técnicas	ARTIGO 240 - SISTEMA S	SENAT
# MAGIC --FEDERAL	Escolas Técnicas	ARTIGO 240 - SISTEMA S	SESCOOP
# MAGIC --FEDERAL	Escolas Técnicas	ARTIGO 240 - SISTEMA S	SESI
# MAGIC --FEDERAL	Escolas Técnicas	ARTIGO 240 - SISTEMA S	null
# MAGIC --FEDERAL	Escolas Técnicas	MILITAR	Aeronáutica
# MAGIC --FEDERAL	Escolas Técnicas	MILITAR	Marinha
# MAGIC --FEDERAL	Escolas de Oferta Exclusiva FIC	ARTIGO 240 - SISTEMA S	SENAC
# MAGIC --FEDERAL	Escolas de Oferta Exclusiva FIC	ARTIGO 240 - SISTEMA S	SENAI
# MAGIC --FEDERAL	Escolas de Oferta Exclusiva FIC	ARTIGO 240 - SISTEMA S	SENAR
# MAGIC --FEDERAL	Escolas de Oferta Exclusiva FIC	ARTIGO 240 - SISTEMA S	SENAT
# MAGIC --FEDERAL	Escolas de Oferta Exclusiva FIC	ARTIGO 240 - SISTEMA S	SESC
# MAGIC --FEDERAL	Escolas de Oferta Outros Cursos	PÚBLICA	null
# MAGIC --FEDERAL	IPES	PRIVADA	null
# MAGIC --MUNICIPAL	Escolas Técnicas	PRIVADA	null
# MAGIC --MUNICIPAL	Escolas Técnicas	PÚBLICA	null

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: escolas_sistec - tipo incompatível código unidade de ensino
# MAGIC %sql
# MAGIC
# MAGIC -- 11) Verificando qualidade de dados: escolas_sistec - tipo incompatível código unidade de ensino
# MAGIC
# MAGIC -- Verificando existência de caracteres diferentes de números no campo CODIGO_UNIDADE_DE_ENSINO
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(*) AS linhas_com_caracteres_diferentes_de_numeros
# MAGIC FROM 
# MAGIC     bronze.escolas_sistec
# MAGIC WHERE 
# MAGIC     CODIGO_UNIDADE_DE_ENSINO RLIKE '[^0-9]';
# MAGIC
# MAGIC -- Resultado: nenhuma linha com caracteres diferentes de números na coluna CODIGO_UNIDADE_DE_ENSINO

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: escolas_sistec - tipo incompatível código município
# MAGIC %sql
# MAGIC
# MAGIC -- 12) Verificando qualidade de dados: escolas_sistec - tipo incompatível código município
# MAGIC
# MAGIC -- Verificando existência de caracteres diferentes de números no campo CODIGO_MUNICIPIO
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(*) AS linhas_com_caracteres_diferentes_de_numeros
# MAGIC FROM 
# MAGIC     bronze.escolas_sistec
# MAGIC WHERE 
# MAGIC     CODIGO_MUNICIPIO RLIKE '[^0-9]';
# MAGIC
# MAGIC -- Resultado: nenhuma linha com caracteres diferentes de números na coluna CODIGO_MUNICIPIO

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: cursos_sistec - esquema
# 13) Verificando qualidade de dados: cursos_sistec - esquema

# Listando esquema da tabela cursos_sistec

df_cursos_spark.printSchema

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: cursos_sistec - quantidade de cursos
# MAGIC %sql
# MAGIC
# MAGIC -- 14) Verificando qualidade de dados: cursos_sistec - quantidade de cursos
# MAGIC
# MAGIC -- Listando quantidade de cursos
# MAGIC
# MAGIC SELECT COUNT(CODIGO_CURSO) FROM bronze.cursos_sistec
# MAGIC
# MAGIC -- Resultado: quantidade de cursos = 83.150

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: cursos_sistec - quantidade distinta de cursos
# MAGIC %sql
# MAGIC
# MAGIC -- 15) Verificando qualidade de dados: cursos_sistec - quantidade distinta de cursos
# MAGIC
# MAGIC -- Listando quantidade distinta de cursos
# MAGIC
# MAGIC SELECT COUNT(DISTINCT CODIGO_CURSO) FROM bronze.cursos_sistec
# MAGIC
# MAGIC -- Resultado: quantidade distinta de cursos = 260

# COMMAND ----------

# DBTITLE 1,Verificando qualidade dos dados - cursos_sistec - nulos
# MAGIC %sql 
# MAGIC
# MAGIC -- 16) Verificando qualidade dos dados - cursos_sistec - nulos
# MAGIC
# MAGIC -- Verificando quantidade de ocorrências de nulos por campos
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(CASE WHEN NOME_SUBTIPO_DE_CURSOS IS NULL THEN 1 END) AS NOME_SUBTIPO_DE_CURSOS_nulos,
# MAGIC     COUNT(CASE WHEN CODIGO_CURSO IS NULL THEN 1 END) AS CODIGO_CURSO_nulos,
# MAGIC     COUNT(CASE WHEN CURSO IS NULL THEN 1 END) AS CURSO_nulos,
# MAGIC     COUNT(CASE WHEN EIXO_TECNOLOGICO IS NULL THEN 1 END) AS EIXO_TECNOLOGICO_nulos,
# MAGIC     COUNT(CASE WHEN MODALIDADE IS NULL THEN 1 END) AS MODALIDADE_nulos,
# MAGIC     COUNT(CASE WHEN CARGA_HORARIA_CURSO IS NULL THEN 1 END) AS CARGA_HORARIA_CURSO_nulos,
# MAGIC     COUNT(CASE WHEN SITUACAO_ATIVO IS NULL THEN 1 END) AS SITUACAO_ATIVO_nulos,
# MAGIC     COUNT(CASE WHEN CODIGO_UNIDADE_DE_ENSINO IS NULL THEN 1 END) AS CODIGO_UNIDADE_DE_ENSINO_nulos,
# MAGIC     COUNT(CASE WHEN UNIDADE_DE_ENSINO IS NULL THEN 1 END) AS UNIDADE_DE_ENSINO_nulos,
# MAGIC     COUNT(CASE WHEN UF IS NULL THEN 1 END) AS UF_nulos,
# MAGIC     COUNT(CASE WHEN CODIGO_MUNICIPIO IS NULL THEN 1 END) AS CODIGO_MUNICIPIO_nulos,
# MAGIC     COUNT(CASE WHEN MUNICIPIO IS NULL THEN 1 END) AS MUNICIPIO_nulos
# MAGIC FROM bronze.cursos_sistec
# MAGIC
# MAGIC -- Resultado: nenhuma coluna possui valores nulos

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: cursos_sistec - domínios
# MAGIC %sql
# MAGIC
# MAGIC -- 17) Verificando qualidade de dados: cursos_sistec - domínios
# MAGIC
# MAGIC -- Todas as consultas para listar domínios dos campos de interesse do estudo na tabela cursos_sistec estão nessa célula; deve-se retirar os caracteres de comentários quando for executar cada comando específico
# MAGIC
# MAGIC --SELECT DISTINCT NOME_SUBTIPO_DE_CURSOS FROM bronze.cursos_sistec
# MAGIC --Resultado:
# MAGIC --Técnico
# MAGIC
# MAGIC --SELECT DISTINCT EIXO_TECNOLOGICO FROM bronze.cursos_sistec
# MAGIC --Resultado:
# MAGIC --Produção Cultural e Design
# MAGIC --Desenvolvimento Educacional e Social
# MAGIC --Produção Industrial
# MAGIC --Infraestrutura
# MAGIC --Controle e Processos Industriais
# MAGIC --Turismo, Hospitalidade e Lazer
# MAGIC --Recursos Naturais
# MAGIC --Ambiente e Saúde
# MAGIC --Informação e Comunicação
# MAGIC --Gestão e Negócios
# MAGIC --Militar
# MAGIC --Produção Alimentícia
# MAGIC --Segurança
# MAGIC
# MAGIC --SELECT DISTINCT MODALIDADE FROM bronze.cursos_sistec
# MAGIC --Resultado:
# MAGIC --Todos
# MAGIC --Educação Presencial
# MAGIC --Educação a Distância
# MAGIC
# MAGIC --SELECT DISTINCT CARGA_HORARIA_CURSO FROM bronze.cursos_sistec
# MAGIC --Resultado:
# MAGIC --800
# MAGIC --1000
# MAGIC --1200
# MAGIC
# MAGIC SELECT DISTINCT SITUACAO_ATIVO FROM bronze.cursos_sistec
# MAGIC --Resultado:
# MAGIC --t
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: cursos_sistec - tipo incompatível código curso
# MAGIC %sql
# MAGIC
# MAGIC -- 18) Verificando qualidade de dados: cursos_sistec - tipo incompatível código curso
# MAGIC
# MAGIC -- Verificando existência de caracteres diferentes de números no campo CODIGO_CURSO
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(*) AS linhas_com_caracteres_diferentes_de_numeros
# MAGIC FROM 
# MAGIC     bronze.cursos_sistec
# MAGIC WHERE 
# MAGIC     CODIGO_CURSO RLIKE '[^0-9]';
# MAGIC
# MAGIC -- Resultado: nenhuma linha com caracteres diferentes de números na coluna CODIGO_CURSO

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: cursos_sistec - tipo incompatível código unidade ensino
# MAGIC %sql
# MAGIC
# MAGIC -- 19) Verificando qualidade de dados: cursos_sistec - tipo incompatível código unidade ensino
# MAGIC
# MAGIC -- Verificando existência de caracteres diferentes de números no campo CODIGO_UNIDADE_DE_ENSINO
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(*) AS linhas_com_caracteres_diferentes_de_numeros
# MAGIC FROM 
# MAGIC     bronze.cursos_sistec
# MAGIC WHERE 
# MAGIC     CODIGO_UNIDADE_DE_ENSINO RLIKE '[^0-9]';
# MAGIC
# MAGIC -- Resultado: nenhuma linha com caracteres diferentes de números na coluna CODIGO_UNIDADE_DE_ENSINO

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: cursos_sistec - tipo incompatível código município
# MAGIC %sql
# MAGIC
# MAGIC -- 20) Verificando qualidade de dados: cursos_sistec - tipo incompatível código município
# MAGIC
# MAGIC -- Verificando existência de caracteres diferentes de números no campo CODIGO_MUNICIPIO
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(*) AS linhas_com_caracteres_diferentes_de_numeros
# MAGIC FROM 
# MAGIC     bronze.cursos_sistec
# MAGIC WHERE 
# MAGIC     CODIGO_MUNICIPIO RLIKE '[^0-9]';
# MAGIC
# MAGIC -- Resultado: nenhuma linha com caracteres diferentes de números na coluna CODIGO_MUNICIPIO

# COMMAND ----------

# DBTITLE 1,Verificando qualidade de dados: cursos_sistec - tipo incompatível carga horária curso
# MAGIC %sql
# MAGIC
# MAGIC -- 21) Verificando qualidade de dados: cursos_sistec - tipo incompatível carga horária curso
# MAGIC
# MAGIC -- Verificando existência de caracteres diferentes de números no campo CARGA_HORARIA_CURSO
# MAGIC
# MAGIC SELECT 
# MAGIC     COUNT(*) AS linhas_com_caracteres_diferentes_de_numeros
# MAGIC FROM 
# MAGIC     bronze.cursos_sistec
# MAGIC WHERE 
# MAGIC     CARGA_HORARIA_CURSO RLIKE '[^0-9]';
# MAGIC
# MAGIC -- Resultado: nenhuma linha com caracteres diferentes de números na coluna CARGA_HORARIA_CURSO