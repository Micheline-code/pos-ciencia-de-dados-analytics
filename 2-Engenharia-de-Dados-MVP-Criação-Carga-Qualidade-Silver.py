# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **2-Engenharia-de-Dados-MVP-Criação-Carga-Qualidade-Silver**
# MAGIC
# MAGIC Este notebook cria e carrega as tabelas da camada silver a partir das tabelas da camada bronze. Para criação das tabelas "escolas" e "cursos" da camada silver, campos das tabelas correspondentes na camada bronze são selecionados; seus tipos de dados são ajustados; valores texto, booleanos e nulos são tratados. 
# MAGIC
# MAGIC Além disso, verifica-se que dados de unidade de ensino, uf e município presentes em cursos são das unidades de ensino. Por fim, são realizadas algumas análises que mostram que nem todas as unidades de ensino da tabela "cursos" existem na tabela "escolas" e, também, que nem todas as unidades de ensino da tabela "escolas" estão na tabela "cursos". 

# COMMAND ----------

# DBTITLE 1,Criando camada silver
# MAGIC %sql
# MAGIC
# MAGIC -- 2) Criando camada silver
# MAGIC
# MAGIC -- Criando camada silver
# MAGIC CREATE DATABASE silver
# MAGIC

# COMMAND ----------

# DBTITLE 1,Criando tabela escolas com campos selecionados e novos tipos de dados
# MAGIC %sql
# MAGIC
# MAGIC -- 3) Criando tabela escolas com campos selecionados e novos tipos de dados
# MAGIC
# MAGIC CREATE TABLE silver.escolas (
# MAGIC     NOME_SISTEMA_DE_ENSINO VARCHAR(255),
# MAGIC     NOME_TIPO_ESCOLA VARCHAR(255),
# MAGIC     CODIGO_UNIDADE_DE_ENSINO INT,
# MAGIC     UNIDADE_DE_ENSINO VARCHAR(255),
# MAGIC     NOME_DEPENDENCIA_ADMINISTRATIVA VARCHAR(255),
# MAGIC     NOME_SUBDEPENDENCIA_ADMINISTRATIVA VARCHAR(255),
# MAGIC     UF CHAR(2),
# MAGIC     CODIGO_MUNICIPIO INT,
# MAGIC     MUNICIPIO VARCHAR(255)
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Povoando tabela escolas com dados tratados da camada bronze
# MAGIC %sql
# MAGIC
# MAGIC -- 4) Povoando tabela escolas com dados tratados da camada bronze
# MAGIC
# MAGIC INSERT INTO silver.escolas (NOME_SISTEMA_DE_ENSINO, NOME_TIPO_ESCOLA, CODIGO_UNIDADE_DE_ENSINO, UNIDADE_DE_ENSINO, NOME_DEPENDENCIA_ADMINISTRATIVA, NOME_SUBDEPENDENCIA_ADMINISTRATIVA, UF, CODIGO_MUNICIPIO, MUNICIPIO)
# MAGIC SELECT NOME_SISTEMA_DE_ENSINO, NOME_TIPO_ESCOLA, int(CODIGO_UNIDADE_DE_ENSINO), UPPER(UNIDADE_DE_ENSINO), NOME_DEPENDENCIA_ADMINISTRATIVA, (CASE WHEN NOME_SUBDEPENDENCIA_ADMINISTRATIVA = 'null' THEN 'NA' ELSE NOME_SUBDEPENDENCIA_ADMINISTRATIVA END) AS NOME_SUBDEPENDENCIA_ADMINISTRATIVA_novo, UF, int(CODIGO_MUNICIPIO), UPPER(MUNICIPIO) FROM bronze.escolas_sistec

# COMMAND ----------

# DBTITLE 1,Criando tabela cursos com campos selecionados e novos tipos de dados
# MAGIC %sql
# MAGIC
# MAGIC -- 5) Criando tabela cursos com campos selecionados e novos tipos de dados
# MAGIC
# MAGIC CREATE TABLE silver.cursos (
# MAGIC     NOME_SUBTIPO_DE_CURSOS VARCHAR(255),
# MAGIC     CODIGO_CURSO INT,
# MAGIC     CURSO VARCHAR(255),
# MAGIC     EIXO_TECNOLOGICO VARCHAR(255),
# MAGIC     MODALIDADE VARCHAR(100),
# MAGIC     CARGA_HORARIA_CURSO INT,
# MAGIC     SITUACAO_ATIVO BOOLEAN,
# MAGIC     CODIGO_UNIDADE_DE_ENSINO INT,
# MAGIC     UNIDADE_DE_ENSINO VARCHAR(255),
# MAGIC     UF CHAR(2),
# MAGIC     CODIGO_MUNICIPIO INT,
# MAGIC     MUNICIPIO VARCHAR(255)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Povoando tabela cursos com dados tratados da camada bronze
# MAGIC %sql
# MAGIC
# MAGIC -- 6) Povoando tabela cursos com dados tratados da camada bronze
# MAGIC
# MAGIC INSERT INTO silver.cursos (NOME_SUBTIPO_DE_CURSOS, CODIGO_CURSO, CURSO, EIXO_TECNOLOGICO, MODALIDADE, CARGA_HORARIA_CURSO,SITUACAO_ATIVO, CODIGO_UNIDADE_DE_ENSINO, UNIDADE_DE_ENSINO, UF, CODIGO_MUNICIPIO, MUNICIPIO)
# MAGIC SELECT NOME_SUBTIPO_DE_CURSOS, int(CODIGO_CURSO), CURSO, EIXO_TECNOLOGICO, MODALIDADE, int(CARGA_HORARIA_CURSO), (CASE WHEN SITUACAO_ATIVO = 't' THEN CAST('TRUE' AS BOOLEAN) ELSE CAST('FALSE' AS BOOLEAN) END) AS SITUACAO_ATIVO_novo, int(CODIGO_UNIDADE_DE_ENSINO), UPPER(UNIDADE_DE_ENSINO), UF, int(CODIGO_MUNICIPIO), UPPER(MUNICIPIO) FROM bronze.cursos_sistec

# COMMAND ----------

# DBTITLE 1,Avaliando campos em comum entre tabelas cursos e escolas - UNIDADE_DE_ENSINO
# MAGIC %sql
# MAGIC
# MAGIC -- 7) Avaliando campos em comum entre tabelas cursos e escolas - UNIDADE_DE_ENSINO
# MAGIC
# MAGIC SELECT * FROM silver.cursos sc
# MAGIC INNER JOIN silver.escolas se ON sc.CODIGO_UNIDADE_DE_ENSINO = se.CODIGO_UNIDADE_DE_ENSINO
# MAGIC WHERE sc.UNIDADE_DE_ENSINO <> se.UNIDADE_DE_ENSINO
# MAGIC
# MAGIC -- Resultado: Unidade de Ensino da tabela de cursos (silver) é a mesma da tabela de escolas (silver)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Avaliando campos em comum entre tabelas cursos e escolas - UF
# MAGIC %sql
# MAGIC
# MAGIC -- 8) Avaliando campos em comum entre tabelas cursos e escolas - UF
# MAGIC
# MAGIC SELECT * FROM silver.cursos sc
# MAGIC INNER JOIN silver.escolas se ON sc.CODIGO_UNIDADE_DE_ENSINO = se.CODIGO_UNIDADE_DE_ENSINO
# MAGIC WHERE sc.UF <> se.UF
# MAGIC
# MAGIC -- Resultado: UF da tabela de cursos (silver) é o mesmo da tabela de escolas (silver)

# COMMAND ----------

# DBTITLE 1,Avaliando campos em comum entre tabelas cursos e escolas - MUNICIPIO
# MAGIC %sql
# MAGIC
# MAGIC -- 9) Avaliando campos em comum entre tabelas cursos e escolas - MUNICIPIO
# MAGIC
# MAGIC SELECT * FROM silver.cursos sc
# MAGIC INNER JOIN silver.escolas se ON sc.CODIGO_UNIDADE_DE_ENSINO = se.CODIGO_UNIDADE_DE_ENSINO
# MAGIC WHERE sc.MUNICIPIO <> se.MUNICIPIO
# MAGIC
# MAGIC -- Resultado: Município da tabela de cursos (silver) é o mesmo da tabela de escolas (silver)

# COMMAND ----------

# DBTITLE 1,Avaliando campos em comum entre tabelas cursos e escolas - CODIGO_MUNICIPIO
# MAGIC %sql
# MAGIC
# MAGIC -- 10) Avaliando campos em comum entre tabelas cursos e escolas - CODIGO_MUNICIPIO
# MAGIC
# MAGIC SELECT * FROM silver.cursos sc
# MAGIC INNER JOIN silver.escolas se ON sc.CODIGO_UNIDADE_DE_ENSINO = se.CODIGO_UNIDADE_DE_ENSINO
# MAGIC WHERE sc.CODIGO_MUNICIPIO <> se.CODIGO_MUNICIPIO
# MAGIC
# MAGIC -- Resultado: Código do Município da tabela de cursos (silver) é o mesmo da tabela de escolas (silver)

# COMMAND ----------

# DBTITLE 1,Avaliando se todas as unidades de ensino da tabela cursos estão na tabela escolas
# MAGIC %sql
# MAGIC
# MAGIC -- 11) Avaliando se todas as unidades de ensino da tabela cursos estão na tabela escolas
# MAGIC
# MAGIC SELECT count(*) FROM silver.cursos where CODIGO_UNIDADE_DE_ENSINO NOT IN (SELECT CODIGO_UNIDADE_DE_ENSINO FROM silver.escolas)
# MAGIC
# MAGIC -- Resultado: 13 unidades de ensino da tabela cursos (silver) não foram encontradas na tabela escolas (silver) e, portanto, não possuem informações complementares próprias da tabela escolas
# MAGIC

# COMMAND ----------

# DBTITLE 1,Avaliando se todas as unidades de ensino da tabela escolas estão na tabela cursos
# MAGIC %sql
# MAGIC
# MAGIC -- 12) Avaliando se todas as unidades de ensino da tabela escolas estão na tabela cursos
# MAGIC
# MAGIC SELECT count(*) FROM silver.escolas where CODIGO_UNIDADE_DE_ENSINO NOT IN (SELECT CODIGO_UNIDADE_DE_ENSINO FROM silver.cursos)
# MAGIC
# MAGIC -- Resultado: 8.093 unidades de ensino da tabela escolas (silver) não foram encontradas na tabela cursos (silver) e, portanto, não possuem cursos relacionados
# MAGIC