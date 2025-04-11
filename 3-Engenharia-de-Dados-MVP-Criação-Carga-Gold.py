# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **3-Engenharia-de-Dados-MVP-Criação-Carga-Gold**
# MAGIC
# MAGIC Este notebook cria e carrega as tabelas da camada golden a partir das tabelas da camada silver. A tabela "cursos_final" é criada a partir da tabela "cursos" (camada silver), acrescida de campos que descrevem unidade de ensino e são provenientes da tabela "escolas" (camada silver). Não serão carregados cursos, cuja unidade de ensino não exista na tabela "escolas" (camada silver).
# MAGIC
# MAGIC A tabela "escolas_final", por sua vez, é criada a partir da tabela "escolas" (camada silver) e de uma tabela temporária da camada golden que possui quantitativos de cursos por unidade de ensino. Essa tabela, "escolas_qtd_cursos", é apagada ao final.

# COMMAND ----------

# DBTITLE 1,Criando camada gold
# MAGIC %sql
# MAGIC
# MAGIC -- 2) Criando camada gold
# MAGIC
# MAGIC -- criando camada gold
# MAGIC CREATE DATABASE gold
# MAGIC

# COMMAND ----------

# DBTITLE 1,Criando tabela cursos_final com campos selecionados e detalhes da unidade de ensino
# MAGIC %sql
# MAGIC
# MAGIC -- 3) Criando tabela cursos_final com campos selecionados e detalhes da unidade de ensino
# MAGIC
# MAGIC -- Como as informações de UNIDADE_DE_ENSINO, UF, CODIGO_MUNICIPIO e MUNICIPIO são provenientes da tabela escolas (silver), será mantido apenas um campo para cada na tabela cursos_final (gold), resultado da junção entre cursos (silver) e escolas (silver); os campos de mesmo nome na tabela cursos (silver) serão desconsiderados.
# MAGIC
# MAGIC CREATE TABLE gold.cursos_final (
# MAGIC     NOME_SUBTIPO_DE_CURSOS VARCHAR(255),
# MAGIC     CODIGO_CURSO INT,
# MAGIC     CURSO VARCHAR(255),
# MAGIC     EIXO_TECNOLOGICO VARCHAR(255),
# MAGIC     MODALIDADE VARCHAR(100),
# MAGIC     CARGA_HORARIA_CURSO INT,
# MAGIC     SITUACAO_ATIVO BOOLEAN,
# MAGIC     NOME_SISTEMA_DE_ENSINO VARCHAR(255),
# MAGIC     NOME_TIPO_ESCOLA VARCHAR(255),
# MAGIC     CODIGO_UNIDADE_DE_ENSINO INT,
# MAGIC     UNIDADE_DE_ENSINO VARCHAR(255),
# MAGIC     NOME_DEPENDENCIA_ADMINISTRATIVA VARCHAR(255),
# MAGIC     NOME_SUBDEPENDENCIA_ADMINISTRATIVA VARCHAR(255),
# MAGIC     UF CHAR(2),
# MAGIC     CODIGO_MUNICIPIO INT,
# MAGIC     MUNICIPIO VARCHAR(255)
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,Povoando tabela cursos_final com dados da junção de cursos e escolas
# MAGIC %sql 
# MAGIC
# MAGIC -- 4) Povoando tabela cursos_final com dados da junção de cursos e escolas
# MAGIC
# MAGIC INSERT INTO gold.cursos_final (
# MAGIC   NOME_SUBTIPO_DE_CURSOS,
# MAGIC   CODIGO_CURSO,
# MAGIC   CURSO,
# MAGIC   EIXO_TECNOLOGICO,
# MAGIC   MODALIDADE,
# MAGIC   CARGA_HORARIA_CURSO,
# MAGIC   SITUACAO_ATIVO,
# MAGIC   NOME_SISTEMA_DE_ENSINO,
# MAGIC   NOME_TIPO_ESCOLA,
# MAGIC   CODIGO_UNIDADE_DE_ENSINO,
# MAGIC   UNIDADE_DE_ENSINO,
# MAGIC   NOME_DEPENDENCIA_ADMINISTRATIVA,
# MAGIC   NOME_SUBDEPENDENCIA_ADMINISTRATIVA,
# MAGIC   UF,
# MAGIC   CODIGO_MUNICIPIO,
# MAGIC   MUNICIPIO)
# MAGIC SELECT 
# MAGIC   sc.NOME_SUBTIPO_DE_CURSOS,
# MAGIC   sc.CODIGO_CURSO,
# MAGIC   sc.CURSO,
# MAGIC   sc.EIXO_TECNOLOGICO,
# MAGIC   sc.MODALIDADE,
# MAGIC   sc.CARGA_HORARIA_CURSO,
# MAGIC   sc.SITUACAO_ATIVO,
# MAGIC   se.NOME_SISTEMA_DE_ENSINO,
# MAGIC   se.NOME_TIPO_ESCOLA,
# MAGIC   se.CODIGO_UNIDADE_DE_ENSINO,
# MAGIC   se.UNIDADE_DE_ENSINO,
# MAGIC   se.NOME_DEPENDENCIA_ADMINISTRATIVA,
# MAGIC   se.NOME_SUBDEPENDENCIA_ADMINISTRATIVA,
# MAGIC   se.UF,
# MAGIC   se.CODIGO_MUNICIPIO,
# MAGIC   se.MUNICIPIO
# MAGIC FROM silver.cursos sc
# MAGIC INNER JOIN silver.escolas se ON sc.CODIGO_UNIDADE_DE_ENSINO = se.CODIGO_UNIDADE_DE_ENSINO
# MAGIC

# COMMAND ----------

# DBTITLE 1,Criando tabela temporária escolas_qtd_cursos com quantitativos de cursos
# MAGIC %sql
# MAGIC
# MAGIC -- 5) Criando tabela temporária escolas_qtd_cursos com quantitativos de cursos
# MAGIC
# MAGIC CREATE TABLE gold.escolas_qtd_cursos (
# MAGIC     CODIGO_UNIDADE_DE_ENSINO INT,
# MAGIC     QTD_CURSOS_PRESENCIAIS INT,
# MAGIC     QTD_CURSOS_DISTANCIA INT,
# MAGIC     QTD_CURSOS_TOTAIS INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO gold.escolas_qtd_cursos (
# MAGIC         CODIGO_UNIDADE_DE_ENSINO, 
# MAGIC         QTD_CURSOS_PRESENCIAIS, 
# MAGIC         QTD_CURSOS_DISTANCIA, 
# MAGIC         QTD_CURSOS_TOTAIS)
# MAGIC SELECT se.CODIGO_UNIDADE_DE_ENSINO, 
# MAGIC         COUNT(CASE WHEN sc.MODALIDADE IN ('Educação Presencial', 'Todos') THEN 1 END) AS QTD_CURSOS_PRESENCIAIS,
# MAGIC         COUNT(CASE WHEN sc.MODALIDADE IN ('Educação a Distância', 'Todos') THEN 1 END) AS QTD_CURSOS_DISTANCIA,
# MAGIC         COUNT(sc.CODIGO_CURSO) AS QTD_CURSOS_TOTAIS
# MAGIC FROM silver.escolas se
# MAGIC LEFT JOIN silver.cursos sc ON sc.CODIGO_UNIDADE_DE_ENSINO = se.CODIGO_UNIDADE_DE_ENSINO
# MAGIC GROUP BY se.CODIGO_UNIDADE_DE_ENSINO
# MAGIC ORDER BY QTD_CURSOS_TOTAIS DESC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Criando tabela escolas_final com campos selecionados e quantitativos de cursos
# MAGIC %sql
# MAGIC
# MAGIC -- 6) Criando tabela escolas_final com campos selecionados e quantitativos de cursos
# MAGIC
# MAGIC CREATE TABLE gold.escolas_final (
# MAGIC     NOME_SISTEMA_DE_ENSINO VARCHAR(255),
# MAGIC     NOME_TIPO_ESCOLA VARCHAR(255),
# MAGIC     CODIGO_UNIDADE_DE_ENSINO INT,
# MAGIC     UNIDADE_DE_ENSINO VARCHAR(255),
# MAGIC     NOME_DEPENDENCIA_ADMINISTRATIVA VARCHAR(255),
# MAGIC     NOME_SUBDEPENDENCIA_ADMINISTRATIVA VARCHAR(255),
# MAGIC     UF CHAR(2),
# MAGIC     CODIGO_MUNICIPIO INT,
# MAGIC     MUNICIPIO VARCHAR(255),
# MAGIC     QTD_CURSOS_PRESENCIAIS INT, 
# MAGIC     QTD_CURSOS_DISTANCIA INT, 
# MAGIC     QTD_CURSOS_TOTAIS INT
# MAGIC )
# MAGIC

# COMMAND ----------

# DBTITLE 1,Povoando tabela escolas_final com dados da junção de escolas e escolas_qtd_cursos (temporária)
# MAGIC %sql
# MAGIC
# MAGIC -- 7) Povoando tabela escolas_final com dados da junção de escolas e escolas_qtd_cursos (temporária)
# MAGIC
# MAGIC INSERT INTO gold.escolas_final (
# MAGIC     NOME_SISTEMA_DE_ENSINO,
# MAGIC     NOME_TIPO_ESCOLA,
# MAGIC     CODIGO_UNIDADE_DE_ENSINO,
# MAGIC     UNIDADE_DE_ENSINO,
# MAGIC     NOME_DEPENDENCIA_ADMINISTRATIVA,
# MAGIC     NOME_SUBDEPENDENCIA_ADMINISTRATIVA,
# MAGIC     UF,
# MAGIC     CODIGO_MUNICIPIO,
# MAGIC     MUNICIPIO,
# MAGIC     QTD_CURSOS_PRESENCIAIS, 
# MAGIC     QTD_CURSOS_DISTANCIA, 
# MAGIC     QTD_CURSOS_TOTAIS
# MAGIC )
# MAGIC SELECT
# MAGIC     se.NOME_SISTEMA_DE_ENSINO,
# MAGIC     se.NOME_TIPO_ESCOLA,
# MAGIC     se.CODIGO_UNIDADE_DE_ENSINO,
# MAGIC     se.UNIDADE_DE_ENSINO,
# MAGIC     se.NOME_DEPENDENCIA_ADMINISTRATIVA,
# MAGIC     se.NOME_SUBDEPENDENCIA_ADMINISTRATIVA,
# MAGIC     se.UF,
# MAGIC     se.CODIGO_MUNICIPIO,
# MAGIC     se.MUNICIPIO,
# MAGIC     gec.QTD_CURSOS_PRESENCIAIS, 
# MAGIC     gec.QTD_CURSOS_DISTANCIA, 
# MAGIC     gec.QTD_CURSOS_TOTAIS
# MAGIC FROM silver.escolas se LEFT JOIN gold.escolas_qtd_cursos gec ON gec.CODIGO_UNIDADE_DE_ENSINO = se.CODIGO_UNIDADE_DE_ENSINO
# MAGIC

# COMMAND ----------

# DBTITLE 1,Apagando tabela temporária escolas_qtd_cursos
# MAGIC %sql
# MAGIC
# MAGIC -- 8) Apagando tabela temporária escolas_qtd_cursos
# MAGIC
# MAGIC DROP TABLE gold.escolas_qtd_cursos
# MAGIC