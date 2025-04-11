# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **4-Engenharia-de-Dados-MVP-Análise-Dados-Escolas**
# MAGIC
# MAGIC A partir de dados públicos e disponíveis sobre unidades de ensino e cursos registrados no SISTEC, é possível mapear a rede de ensino e a oferta de cursos técnicos a partir de vários recortes. Além disso, sua distribuição por estados e municípios pode servir de base para planejar a formação de novos profissionais alinhada às demandas de cada região.
# MAGIC
# MAGIC **Diante disso, o objetivo deste notebook consiste em responder as seguintes questões sobre Unidades de Ensino.**
# MAGIC
# MAGIC - Quais os 5 estados com maior e menor quantidade de unidades de ensino?
# MAGIC - Quais as 5 capitais com maior e menor quantidade de unidades de ensino?
# MAGIC - Quais os 5 estados com maior e menor quantidade de unidades de ensino em municípios diferentes de capitais?
# MAGIC - Como está a distribuição da quantidade de unidades de ensino por sistema de ensino, tipo de escola, dependência administrativa e sub-dependência administrativa?
# MAGIC - Quais as 5 unidades de ensino com maior e menor quantidade de cursos presenciais? Em quais municípios e estados essas unidades estão?
# MAGIC - Quais as 5 unidades de ensino com a maior e menor quantidade de cursos à distância? Em quais municípios e estados essas unidades estão?
# MAGIC - Existem unidades de ensino sem oferta de cursos? Em que estados elas estão?
# MAGIC - Qual o percentual de unidades de ensino com e sem cursos por UF?
# MAGIC - Qual o percentual de unidades de ensino em capitais e fora das capitais por UF?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports para visualizações
import pandas as pd
import plotly.express as px
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,5 estados com maior quantidade de unidades de ensino

# Quais os 5 estados com maior quantidade de unidades de ensino?

df_5_maiores_qtd_unidades_uf = spark.sql('SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL GROUP BY UF ORDER BY QTD_UNIDADES DESC LIMIT 5')

df_5_maiores_qtd_unidades_uf_pd = df_5_maiores_qtd_unidades_uf.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_5_maiores_qtd_unidades_uf_pd,
    x='UF',
    y='QTD_UNIDADES',
    labels={"UF": "Unidade Federativa", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Ranking de Quantidade de Unidades de Ensino por UF (5 maiores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_5_maiores_qtd_unidades_uf_pd['QTD_UNIDADES'],
    textposition="inside",
    texttemplate='%{text}'
)

fig.show()

# Resultado: no ranking dos 5 estados com maior quantidade de unidades de ensino, os 4 primeiros estão nas regiões Sudeste e Sul (SP, MG, PR, RJ, BA)


# COMMAND ----------

# DBTITLE 1,5 estados com menor quantidade de unidades de ensino

# Quais os 5 estados com menor quantidade de unidades de ensino?

df_5_menores_qtd_unidades_uf = spark.sql('SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL GROUP BY UF ORDER BY QTD_UNIDADES ASC LIMIT 5')

df_5_menores_qtd_unidades_uf_pd = df_5_menores_qtd_unidades_uf.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_5_menores_qtd_unidades_uf_pd,
    x='UF',
    y='QTD_UNIDADES',
    labels={"UF": "Unidade Federativa", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Ranking de Quantidade de Unidades de Ensino por UF (5 menores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_5_menores_qtd_unidades_uf_pd['QTD_UNIDADES'],
    textposition="inside",
    texttemplate='%{text}'
)

fig.show()

# Resultado: no ranking dos 5 estados com menor quantidade de unidades de ensino, os 4 primeiros estão na região Norte (AP, AC, RR, RO AL)


# COMMAND ----------

# DBTITLE 1,5 capitais com maior quantidade de unidades de ensino
# Quais as 5 capitais com maior quantidade de unidades de ensino?

df_5_maiores_qtd_unidades_capital = spark.sql("SELECT MUNICIPIO, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL WHERE MUNICIPIO IN ('RIO BRANCO', 'MACEIÓ', 'MACAPÁ', 'MANAUS', 'SALVADOR', 'FORTALEZA', 'BRASÍLIA', 'VITÓRIA', 'GOIÂNIA', 'SÃO LUÍS', 'CUIABÁ', 'CAMPO BRANCO', 'BELO HORIZONTE', 'BELÉM', 'JOÃO PESSOA', 'CURITIBA', 'RECIFE', 'TERESINA', 'RIO DE JANEIRO', 'NATAL', 'PORTO ALEGRE', 'PORTO VELHO', 'BOA VISTA', 'FLORIANÓPOLIS', 'SÃO PAULO', 'ARACAJÚ', 'PALMAS') GROUP BY MUNICIPIO ORDER BY QTD_UNIDADES DESC LIMIT 5")

df_5_maiores_qtd_unidades_capital_pd = df_5_maiores_qtd_unidades_capital.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_5_maiores_qtd_unidades_capital_pd,
    x='MUNICIPIO',
    y='QTD_UNIDADES',
    labels={"MUNICIPIO": "Capital", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Ranking de Quantidade de Unidades de Ensino por Capital (5 maiores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_5_maiores_qtd_unidades_capital_pd['QTD_UNIDADES'],
    textposition="inside",
    texttemplate='%{text}'
)

fig.show()

# Resultado: no ranking das 5 capitais com maior quantidade de unidades de ensino, as 4 primeiras estão nas regiões Sudeste e Sul

# COMMAND ----------

# DBTITLE 1,5 capitais com menor quantidade de unidades de ensino
# Quais as 5 capitais com menor quantidade de unidades de ensino?

df_5_menores_qtd_unidades_capital = spark.sql("SELECT MUNICIPIO, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL WHERE MUNICIPIO IN ('RIO BRANCO', 'MACEIÓ', 'MACAPÁ', 'MANAUS', 'SALVADOR', 'FORTALEZA', 'BRASÍLIA', 'VITÓRIA', 'GOIÂNIA', 'SÃO LUÍS', 'CUIABÁ', 'CAMPO BRANCO', 'BELO HORIZONTE', 'BELÉM', 'JOÃO PESSOA', 'CURITIBA', 'RECIFE', 'TERESINA', 'RIO DE JANEIRO', 'NATAL', 'PORTO ALEGRE', 'PORTO VELHO', 'BOA VISTA', 'FLORIANÓPOLIS', 'SÃO PAULO', 'ARACAJÚ', 'PALMAS') GROUP BY MUNICIPIO ORDER BY QTD_UNIDADES ASC LIMIT 5")

df_5_menores_qtd_unidades_capital_pd = df_5_menores_qtd_unidades_capital.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_5_menores_qtd_unidades_capital_pd,
    x='MUNICIPIO',
    y='QTD_UNIDADES',
    labels={"MUNICIPIO": "Capital", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Ranking de Quantidade de Unidades de Ensino por Capital (5 menores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_5_menores_qtd_unidades_capital_pd['QTD_UNIDADES'],
    textposition="inside",
    texttemplate='%{text}'
)

fig.show()

# Resultado: no ranking dos 5 capitais com menor quantidade de unidades de ensino, as 4 primeiras estão na região Norte

# COMMAND ----------

# DBTITLE 1,5 estados com maior quantidade de unidades de ensino em municípios diferentes de capitais
# MAGIC %sql
# MAGIC
# MAGIC -- Quais os 5 estados com maior quantidade de unidades de ensino em municípios diferentes de capitais?
# MAGIC
# MAGIC SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL WHERE MUNICIPIO NOT IN ('RIO BRANCO', 'MACEIÓ', 'MACAPÁ', 'MANAUS', 'SALVADOR', 'FORTALEZA', 'BRASÍLIA', 'VITÓRIA', 'GOIÂNIA', 'SÃO LUÍS', 'CUIABÁ', 'CAMPO BRANCO', 'BELO HORIZONTE', 'BELÉM', 'JOÃO PESSOA', 'CURITIBA', 'RECIFE', 'TERESINA', 'RIO DE JANEIRO', 'NATAL', 'PORTO ALEGRE', 'PORTO VELHO', 'BOA VISTA', 'FLORIANÓPOLIS', 'SÃO PAULO', 'ARACAJÚ', 'PALMAS') GROUP BY UF ORDER BY QTD_UNIDADES DESC LIMIT 5
# MAGIC
# MAGIC -- Resultado: no ranking dos 5 estados com maior quantidade de unidades de ensino em municípios diferentes de capitais, os 4 primeiros estão nas regiões Sudeste e Sul (SP, MG, PR, RS, BA)
# MAGIC

# COMMAND ----------

# DBTITLE 1,5 estados com menor quantidade de unidades de ensino em municípios diferentes de capitais
# MAGIC %sql
# MAGIC
# MAGIC -- Quais os 5 estados com menor quantidade de unidades de ensino em municípios diferentes de capitais?
# MAGIC
# MAGIC SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL WHERE MUNICIPIO NOT IN ('RIO BRANCO', 'MACEIÓ', 'MACAPÁ', 'MANAUS', 'SALVADOR', 'FORTALEZA', 'BRASÍLIA', 'VITÓRIA', 'GOIÂNIA', 'SÃO LUÍS', 'CUIABÁ', 'CAMPO BRANCO', 'BELO HORIZONTE', 'BELÉM', 'JOÃO PESSOA', 'CURITIBA', 'RECIFE', 'TERESINA', 'RIO DE JANEIRO', 'NATAL', 'PORTO ALEGRE', 'PORTO VELHO', 'BOA VISTA', 'FLORIANÓPOLIS', 'SÃO PAULO', 'ARACAJÚ', 'PALMAS') GROUP BY UF ORDER BY QTD_UNIDADES ASC LIMIT 5
# MAGIC
# MAGIC -- Resultado: no ranking dos 5 estados com menor quantidade de unidades de ensino em municípios diferentes de capitais, os 4 primeiros estão na região Norte (RR, AP, AC, AM, AL)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de unidades de ensino por Sistema de Ensino
# Como está a distribuição da quantidade de unidades de ensino por sistema de ensino?

df_distribuicao_sistema_ensino = spark.sql('SELECT NOME_SISTEMA_DE_ENSINO, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL GROUP BY NOME_SISTEMA_DE_ENSINO ORDER BY QTD_UNIDADES DESC')

df_distribuicao_sistema_ensino_pd = df_distribuicao_sistema_ensino.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_sistema_ensino_pd,
    names='NOME_SISTEMA_DE_ENSINO',
    values='QTD_UNIDADES',
    labels={"NOME_SISTEMA_DE_ENSINO": "Nome Sistema Ensino", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Distribuição da quantidade de unidades de ensino por Sistema de Ensino"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de unidades de ensino por Nome Tipo Escola
# Como está a distribuição da quantidade de unidades de ensino por tipo de escola?

df_distribuicao_tipo_escola = spark.sql('SELECT NOME_TIPO_ESCOLA, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL GROUP BY NOME_TIPO_ESCOLA ORDER BY QTD_UNIDADES DESC')

df_distribuicao_tipo_escola_pd = df_distribuicao_tipo_escola.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_tipo_escola_pd,
    names='NOME_TIPO_ESCOLA',
    values='QTD_UNIDADES',
    labels={"NOME_TIPO_ESCOLA": "Nome Tipo Escola", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Distribuição da quantidade de unidades de ensino por Tipo de Escola"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de unidades de ensino por Dependência Administrativa
# Como está a distribuição da quantidade de unidades de ensino por dependência administrativa?

df_distribuicao_dependencia_administrativa = spark.sql('SELECT NOME_DEPENDENCIA_ADMINISTRATIVA, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL GROUP BY NOME_DEPENDENCIA_ADMINISTRATIVA ORDER BY QTD_UNIDADES DESC')

df_distribuicao_dependencia_administrativa_pd = df_distribuicao_dependencia_administrativa.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_dependencia_administrativa_pd,
    names='NOME_DEPENDENCIA_ADMINISTRATIVA',
    values='QTD_UNIDADES',
    labels={"NOME_DEPENDENCIA_ADMINISTRATIVA": "Nome Dependência Administrativa", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Distribuição da quantidade de unidades de ensino por Dependência Administrativa"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de unidades de ensino por Sub-Dependência Administrativa
#Como está a distribuição da quantidade de unidades de ensino por sub-dependência administrativa?

df_distribuicao_subdependencia_administrativa = spark.sql('SELECT NOME_SUBDEPENDENCIA_ADMINISTRATIVA, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL WHERE NOME_SUBDEPENDENCIA_ADMINISTRATIVA <> "NA" GROUP BY NOME_SUBDEPENDENCIA_ADMINISTRATIVA ORDER BY QTD_UNIDADES DESC')

df_distribuicao_subdependencia_administrativa_pd = df_distribuicao_subdependencia_administrativa.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_subdependencia_administrativa_pd,
    names='NOME_SUBDEPENDENCIA_ADMINISTRATIVA',
    values='QTD_UNIDADES',
    labels={"NOME_SUBDEPENDENCIA_ADMINISTRATIVA": "Nome Sub-Dependência Administrativa", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Distribuição da quantidade de unidades de ensino por Dependência Administrativa"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,5 Unidades de ensino com maior quantidade de cursos presenciais
# MAGIC %sql
# MAGIC
# MAGIC -- Quais as 5 unidades de ensino com maior quantidade de cursos presenciais? Em quais municípios e estados essas unidades estão?
# MAGIC
# MAGIC SELECT CODIGO_UNIDADE_DE_ENSINO, UNIDADE_DE_ENSINO, UF, MUNICIPIO, QTD_CURSOS_PRESENCIAIS FROM GOLD.ESCOLAS_FINAL WHERE QTD_CURSOS_PRESENCIAIS > 0 ORDER BY QTD_CURSOS_PRESENCIAIS DESC LIMIT 5
# MAGIC

# COMMAND ----------

# DBTITLE 1,5 Unidades de ensino com menor quantidade de cursos presenciais
# MAGIC %sql
# MAGIC
# MAGIC -- Quais as 5 unidades de ensino com menor quantidade de cursos presenciais? Em quais municípios e estados essas unidades estão?
# MAGIC
# MAGIC SELECT CODIGO_UNIDADE_DE_ENSINO, UNIDADE_DE_ENSINO, UF, MUNICIPIO, QTD_CURSOS_PRESENCIAIS FROM GOLD.ESCOLAS_FINAL WHERE QTD_CURSOS_PRESENCIAIS > 0 ORDER BY QTD_CURSOS_PRESENCIAIS ASC LIMIT 5

# COMMAND ----------

# DBTITLE 1,5 Unidades de ensino com maior quantidade de cursos à distância
# MAGIC %sql
# MAGIC
# MAGIC -- Quais as 5 unidades de ensino com maior quantidade de cursos à distância? Em quais municípios e estados essas unidades estão?
# MAGIC
# MAGIC SELECT CODIGO_UNIDADE_DE_ENSINO, UNIDADE_DE_ENSINO, UF, MUNICIPIO, QTD_CURSOS_DISTANCIA FROM GOLD.ESCOLAS_FINAL WHERE QTD_CURSOS_DISTANCIA > 0 ORDER BY QTD_CURSOS_DISTANCIA DESC LIMIT 5
# MAGIC

# COMMAND ----------

# DBTITLE 1,5 Unidades de ensino com menor quantidade de cursos à distância
# MAGIC %sql
# MAGIC
# MAGIC -- Quais as 5 unidades de ensino com menor quantidade de cursos à distância? Em quais municípios e estados essas unidades estão?
# MAGIC
# MAGIC SELECT CODIGO_UNIDADE_DE_ENSINO, UNIDADE_DE_ENSINO, UF, MUNICIPIO, QTD_CURSOS_DISTANCIA FROM GOLD.ESCOLAS_FINAL WHERE QTD_CURSOS_DISTANCIA > 0 ORDER BY QTD_CURSOS_DISTANCIA ASC LIMIT 5
# MAGIC

# COMMAND ----------

# DBTITLE 1,Unidades de ensino sem cursos  por UF
# MAGIC %sql
# MAGIC
# MAGIC -- Existem unidades de ensino sem oferta de cursos? Em que estados elas estão? (visão sem gráfico)
# MAGIC
# MAGIC SELECT UF, COUNT(CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL WHERE QTD_CURSOS_TOTAIS = 0 GROUP BY UF ORDER BY QTD_UNIDADES DESC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Unidades de ensino sem cursos  por UF
# Existem unidades de ensino sem oferta de cursos? Em que estados elas estão? (visão com gráfico)

df_unidades_sem_cursos_uf = spark.sql('SELECT UF, COUNT(CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL WHERE QTD_CURSOS_TOTAIS = 0 GROUP BY UF ORDER BY QTD_UNIDADES DESC')

df_unidades_sem_cursos_uf_pd = df_unidades_sem_cursos_uf.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_unidades_sem_cursos_uf_pd,
    x='UF',
    y='QTD_UNIDADES',
    labels={"UF": "Unidade Federativa", "QTD_UNIDADES": "Quantidade de Unidades"},
    title="Quantidade de Unidades de Ensino sem cursos por UF"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_unidades_sem_cursos_uf_pd['QTD_UNIDADES'],
    textposition="inside",
    texttemplate='%{text}',
    textfont=dict(size=30)
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Percentual de unidades de ensino com e sem cursos por UF
# Qual o percentual de unidades de ensino com e sem cursos por UF?

df_unidades_uf = spark.sql('SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL GROUP BY UF')

df_unidades_com_cursos_uf = spark.sql('SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES_COM_CURSOS FROM GOLD.ESCOLAS_FINAL WHERE QTD_CURSOS_TOTAIS <> 0 GROUP BY UF')

df_unidades_sem_cursos_uf = spark.sql('SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES_SEM_CURSOS FROM GOLD.ESCOLAS_FINAL WHERE QTD_CURSOS_TOTAIS = 0 GROUP BY UF')

df_mapa_uf_1 = (df_unidades_uf
            .join(df_unidades_com_cursos_uf, 'UF', 'left')
            .join(df_unidades_sem_cursos_uf, 'UF', 'left')
            .withColumn('PERC_COM_CURSOS', F.ceil((df_unidades_com_cursos_uf['QTD_UNIDADES_COM_CURSOS']/df_unidades_uf['QTD_UNIDADES'])*100))
            .withColumn('PERC_SEM_CURSOS', F.ceil((df_unidades_sem_cursos_uf['QTD_UNIDADES_SEM_CURSOS']/df_unidades_uf['QTD_UNIDADES'])*100)) 
)

df_mapa_uf_analise_1 = df_mapa_uf_1.fillna(0)

df_mapa_uf_analise_1.display()


# COMMAND ----------

# DBTITLE 1,Análise estatística do dataframe de unidades com e sem cursos

# Análise estatística do dataframe de unidades com e sem cursos

df_mapa_uf_analise_1.describe().show()

# COMMAND ----------

# DBTITLE 1,Percentual de unidades de ensino dentro e fora de capitais por UF
# Qual o percentual de unidades de ensino dentro e fora de capitais por UF?

df_unidades_uf = spark.sql('SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES FROM GOLD.ESCOLAS_FINAL GROUP BY UF')

df_unidades_capitais_uf = spark.sql("SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES_CAPITAIS FROM GOLD.ESCOLAS_FINAL WHERE MUNICIPIO IN ('RIO BRANCO', 'MACEIÓ', 'MACAPÁ', 'MANAUS', 'SALVADOR', 'FORTALEZA', 'BRASÍLIA', 'VITÓRIA', 'GOIÂNIA', 'SÃO LUÍS', 'CUIABÁ', 'CAMPO BRANCO', 'BELO HORIZONTE', 'BELÉM', 'JOÃO PESSOA', 'CURITIBA', 'RECIFE', 'TERESINA', 'RIO DE JANEIRO', 'NATAL', 'PORTO ALEGRE', 'PORTO VELHO', 'BOA VISTA', 'FLORIANÓPOLIS', 'SÃO PAULO', 'ARACAJÚ', 'PALMAS') GROUP BY UF")

df_unidades_nao_capitais_uf = spark.sql("SELECT UF, COUNT(DISTINCT CODIGO_UNIDADE_DE_ENSINO) AS QTD_UNIDADES_NAO_CAPITAIS FROM GOLD.ESCOLAS_FINAL WHERE MUNICIPIO NOT IN ('RIO BRANCO', 'MACEIÓ', 'MACAPÁ', 'MANAUS', 'SALVADOR', 'FORTALEZA', 'BRASÍLIA', 'VITÓRIA', 'GOIÂNIA', 'SÃO LUÍS', 'CUIABÁ', 'CAMPO BRANCO', 'BELO HORIZONTE', 'BELÉM', 'JOÃO PESSOA', 'CURITIBA', 'RECIFE', 'TERESINA', 'RIO DE JANEIRO', 'NATAL', 'PORTO ALEGRE', 'PORTO VELHO', 'BOA VISTA', 'FLORIANÓPOLIS', 'SÃO PAULO', 'ARACAJÚ', 'PALMAS') GROUP BY UF")

df_mapa_uf_2 = (df_unidades_uf
            .join(df_unidades_capitais_uf, 'UF', 'left')
            .join(df_unidades_nao_capitais_uf, 'UF', 'left')
            .withColumn('PERC_CAPITAIS', F.ceil((df_unidades_capitais_uf['QTD_UNIDADES_CAPITAIS']/df_unidades_uf['QTD_UNIDADES'])*100))
            .withColumn('PERC_NAO_CAPITAIS', F.ceil((df_unidades_nao_capitais_uf['QTD_UNIDADES_NAO_CAPITAIS']/df_unidades_uf['QTD_UNIDADES'])*100))            
)

df_mapa_uf_analise_2 = df_mapa_uf_2.fillna(0)

df_mapa_uf_analise_2.display()

# COMMAND ----------

# DBTITLE 1,Análise estatística do dataframe de unidades dentro e fora de capitais
# Análise estatística do dataframe de unidades dentro e fora de capitais

df_mapa_uf_analise_2.describe().show()