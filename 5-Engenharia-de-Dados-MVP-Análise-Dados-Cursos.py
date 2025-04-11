# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC **5-Engenharia-de-Dados-MVP-Análise-Dados-Cursos**
# MAGIC
# MAGIC A partir de dados públicos e disponíveis sobre unidades de ensino e cursos registrados no SISTEC, é possível mapear a rede de ensino e a oferta de cursos técnicos a partir de vários recortes. Além disso, sua distribuição por estados e municípios pode servir de base para planejar a formação de novos profissionais alinhada às demandas de cada região.
# MAGIC
# MAGIC **Diante disso, o objetivo deste notebook consiste em responder as seguintes questões sobre Cursos.**
# MAGIC
# MAGIC - Quais os 5 estados com maior e menor quantidade cursos?
# MAGIC - Como está a distribuição da quantidade de cursos por eixo tecnológico?
# MAGIC - Como está a distribuição da quantidade de cursos por modalidade?
# MAGIC - Como está a distribuição da quantidade de cursos por carga horária?
# MAGIC - Como está a distribuição da quantidade de cursos por sistema de ensino?
# MAGIC - Como está a distribuição da quantidade de cursos por dependência administrativa?
# MAGIC - Quais os 5 cursos presenciais com maior e menor oferta?
# MAGIC - Quais os 5 cursos à distância com maior e menor oferta?
# MAGIC - Qual a distribuição dos cursos por eixo e região?
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports para visualizações
import pandas as pd
import plotly.express as px
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,5 estados com maior quantidade de cursos
# Quais os 5 estados com maior quantidade cursos?

df_5_maiores_qtd_cursos_uf = spark.sql('SELECT UF, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL GROUP BY UF ORDER BY QTD_CURSOS DESC LIMIT 5')

df_5_maiores_qtd_cursos_uf_pd = df_5_maiores_qtd_cursos_uf.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_5_maiores_qtd_cursos_uf_pd,
    x='UF',
    y='QTD_CURSOS',
    labels={"UF": "Unidade Federativa", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Ranking de Quantidade de Cursos por UF (5 maiores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_5_maiores_qtd_cursos_uf_pd['QTD_CURSOS'],
    textposition="inside",
    texttemplate='%{text}',
    textfont=dict(size=12)
)

fig.show()

# Resultado: no ranking dos 5 estados com maior quantidade de cursos, todos estão nas regiões Sudeste e Sul (SP, MG, RJ, PR, RS)


# COMMAND ----------

# DBTITLE 1,5 estados com menor quantidade de cursos
# Quais os 5 estados com menor quantidade cursos?

df_5_menores_qtd_cursos_uf = spark.sql('SELECT UF, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL GROUP BY UF ORDER BY QTD_CURSOS ASC LIMIT 5')

df_5_menores_qtd_cursos_uf_pd = df_5_menores_qtd_cursos_uf.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_5_menores_qtd_cursos_uf_pd,
    x='UF',
    y='QTD_CURSOS',
    labels={"UF": "Unidade Federativa", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Ranking de Quantidade de Cursos por UF (5 menores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_5_menores_qtd_cursos_uf_pd['QTD_CURSOS'],
    textposition="inside",
    texttemplate='%{text}',
    textfont=dict(size=12)
)

fig.show()

# Resultado: no ranking dos 5 estados com menor quantidade de cursos, os 4 primeiros estão na região Norte (RR, AP, AC, TO)


# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de cursos por Eixo Tecnológico
# Como está a distribuição da quantidade de cursos por eixo tecnológico?

df_distribuicao_cursos_eixo = spark.sql('SELECT EIXO_TECNOLOGICO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL GROUP BY EIXO_TECNOLOGICO ORDER BY QTD_CURSOS DESC')

df_distribuicao_cursos_eixo_pd = df_distribuicao_cursos_eixo.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_cursos_eixo_pd,
    names='EIXO_TECNOLOGICO',
    values='QTD_CURSOS',
    labels={"EIXO_TECNOLOGICO": "Eixo Tecnológico", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Distribuição da quantidade de cursos por Eixo Tecnológico"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de cursos por modalidade
# Como está a distribuição da quantidade de cursos por modalidade?

df_distribuicao_cursos_modalidade = spark.sql('SELECT MODALIDADE, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL GROUP BY MODALIDADE ORDER BY QTD_CURSOS DESC')

df_distribuicao_cursos_modalidade_pd = df_distribuicao_cursos_modalidade.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_cursos_modalidade_pd,
    names='MODALIDADE',
    values='QTD_CURSOS',
    labels={"MODALIDADE": "Modalidade", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Distribuição da quantidade de cursos por Modalidade"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de cursos por carga horária
# Como está a distribuição da quantidade de cursos por carga horária?

df_distribuicao_cursos_carga_horaria = spark.sql('SELECT CARGA_HORARIA_CURSO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL GROUP BY CARGA_HORARIA_CURSO ORDER BY QTD_CURSOS DESC')

df_distribuicao_cursos_carga_horaria_pd = df_distribuicao_cursos_carga_horaria.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_cursos_carga_horaria_pd,
    names='CARGA_HORARIA_CURSO',
    values='QTD_CURSOS',
    labels={"CARGA_HORARIA_CURSO": "Carga Horária", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Distribuição da quantidade de cursos por Carga Horária"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de cursos por sistema de ensino
# Como está a distribuição da quantidade de cursos por sistema de ensino?

df_distribuicao_cursos_sistema_ensino = spark.sql('SELECT NOME_SISTEMA_DE_ENSINO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL GROUP BY NOME_SISTEMA_DE_ENSINO ORDER BY QTD_CURSOS DESC')

df_distribuicao_cursos_sistema_ensino_pd = df_distribuicao_cursos_sistema_ensino.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_cursos_sistema_ensino_pd,
    names='NOME_SISTEMA_DE_ENSINO',
    values='QTD_CURSOS',
    labels={"NOME_SISTEMA_DE_ENSINO": "Sistema de Ensino", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Distribuição da quantidade de cursos por Sistema de Ensino"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição da quantidade de cursos por dependência administrativa
# Como está a distribuição da quantidade de cursos por sistema de ensino?

df_distribuicao_cursos_dep_adm = spark.sql('SELECT NOME_DEPENDENCIA_ADMINISTRATIVA, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL GROUP BY NOME_DEPENDENCIA_ADMINISTRATIVA ORDER BY QTD_CURSOS DESC')

df_distribuicao_cursos_dep_adm_pd = df_distribuicao_cursos_dep_adm.toPandas()

# Criando um gráfico de pizza
fig = px.pie(
    df_distribuicao_cursos_dep_adm_pd,
    names='NOME_DEPENDENCIA_ADMINISTRATIVA',
    values='QTD_CURSOS',
    labels={"NOME_DEPENDENCIA_ADMINISTRATIVA": "Dependência Administrativa", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Distribuição da quantidade de cursos por Dependência Administrativa"
)

# Adicionando rótulos
fig.update_traces(
    textinfo='percent+value',
    textposition="outside"
)

fig.show()

# COMMAND ----------

# DBTITLE 1,5 cursos presenciais com maior oferta
# Quais os 5 cursos presenciais com maior oferta?

df_5_maiores_oferta_presencial = spark.sql('SELECT CODIGO_CURSO, CURSO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL WHERE MODALIDADE IN ("Educação Presencial", "Todos") GROUP BY CODIGO_CURSO, CURSO ORDER BY QTD_CURSOS DESC LIMIT 5')

df_5_maiores_oferta_presencial.display()

# COMMAND ----------

# DBTITLE 1,5 cursos presenciais com menor oferta
# Quais os 5 cursos presenciais com menor oferta?

df_5_menores_oferta_presencial = spark.sql('SELECT CODIGO_CURSO, CURSO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL WHERE MODALIDADE IN ("Educação Presencial", "Todos") GROUP BY CODIGO_CURSO, CURSO ORDER BY QTD_CURSOS ASC LIMIT 5')

df_5_menores_oferta_presencial.display()

# COMMAND ----------

# DBTITLE 1,5 cursos à distância com maior oferta
# Quais os 5 cursos à distância com maior oferta?

df_5_maiores_oferta_distancia = spark.sql('SELECT CODIGO_CURSO, CURSO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL WHERE MODALIDADE IN ("Educação a Distância", "Todos") GROUP BY CODIGO_CURSO, CURSO ORDER BY QTD_CURSOS DESC LIMIT 5')

df_5_maiores_oferta_distancia.display()

# COMMAND ----------

# DBTITLE 1,5 cursos à distância com menor oferta
# Quais os 5 cursos à distância com menor oferta?

df_5_menores_oferta_distancia = spark.sql('SELECT CODIGO_CURSO, CURSO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL WHERE MODALIDADE IN ("Educação a Distância", "Todos") GROUP BY CODIGO_CURSO, CURSO ORDER BY QTD_CURSOS ASC LIMIT 5')

df_5_menores_oferta_distancia.display()

# COMMAND ----------

# DBTITLE 1,Distribuição de cursos por eixo região sul
df_distribuicao_cursos_eixo_reg_sul = spark.sql("SELECT EIXO_TECNOLOGICO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL  WHERE UF IN ('SC', 'PR', 'RS') GROUP BY EIXO_TECNOLOGICO ORDER BY QTD_CURSOS DESC LIMIT 5")

df_distribuicao_cursos_eixo_reg_sul_pd = df_distribuicao_cursos_eixo_reg_sul.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_distribuicao_cursos_eixo_reg_sul_pd,
    x='EIXO_TECNOLOGICO',
    y='QTD_CURSOS',
    labels={"EIXO_TECNOLOGICO": "Eixo Tecnológico", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Ranking de Quantidade de Cursos por Eixo Tecnológico - Região Sul (5 maiores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_distribuicao_cursos_eixo_reg_sul_pd['QTD_CURSOS'],
    textposition="inside",
    texttemplate='%{text}',
    textfont=dict(size=12)
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição de cursos por eixo região sudeste
df_distribuicao_cursos_eixo_reg_sudeste = spark.sql("SELECT EIXO_TECNOLOGICO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL  WHERE UF IN ('ES', 'MG', 'RJ', 'SP') GROUP BY EIXO_TECNOLOGICO ORDER BY QTD_CURSOS DESC LIMIT 5")

df_distribuicao_cursos_eixo_reg_sudeste_pd = df_distribuicao_cursos_eixo_reg_sudeste.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_distribuicao_cursos_eixo_reg_sudeste_pd,
    x='EIXO_TECNOLOGICO',
    y='QTD_CURSOS',
    labels={"EIXO_TECNOLOGICO": "Eixo Tecnológico", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Ranking de Quantidade de Cursos por Eixo Tecnológico - Região Sudeste (5 maiores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_distribuicao_cursos_eixo_reg_sudeste_pd['QTD_CURSOS'],
    textposition="inside",
    texttemplate='%{text}',
    textfont=dict(size=12)
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição de cursos por eixo região centro-oeste
df_distribuicao_cursos_eixo_reg_centro = spark.sql("SELECT EIXO_TECNOLOGICO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL  WHERE UF IN ('GO', 'MT', 'MS', 'DF') GROUP BY EIXO_TECNOLOGICO ORDER BY QTD_CURSOS DESC LIMIT 5")

df_distribuicao_cursos_eixo_reg_centro_pd = df_distribuicao_cursos_eixo_reg_centro.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_distribuicao_cursos_eixo_reg_centro_pd,
    x='EIXO_TECNOLOGICO',
    y='QTD_CURSOS',
    labels={"EIXO_TECNOLOGICO": "Eixo Tecnológico", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Ranking de Quantidade de Cursos por Eixo Tecnológico - Região Centro-Oeste (5 maiores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_distribuicao_cursos_eixo_reg_centro_pd['QTD_CURSOS'],
    textposition="inside",
    texttemplate='%{text}',
    textfont=dict(size=12)
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição de cursos por eixo região norte
df_distribuicao_cursos_eixo_reg_norte = spark.sql("SELECT EIXO_TECNOLOGICO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL  WHERE UF IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'PA') GROUP BY EIXO_TECNOLOGICO ORDER BY QTD_CURSOS DESC LIMIT 5")

df_distribuicao_cursos_eixo_reg_norte_pd = df_distribuicao_cursos_eixo_reg_norte.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_distribuicao_cursos_eixo_reg_norte_pd,
    x='EIXO_TECNOLOGICO',
    y='QTD_CURSOS',
    labels={"EIXO_TECNOLOGICO": "Eixo Tecnológico", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Ranking de Quantidade de Cursos por Eixo Tecnológico - Região Norte (5 maiores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_distribuicao_cursos_eixo_reg_norte_pd['QTD_CURSOS'],
    textposition="inside",
    texttemplate='%{text}',
    textfont=dict(size=12)
)

fig.show()

# COMMAND ----------

# DBTITLE 1,Distribuição de cursos por eixo região nordeste
df_distribuicao_cursos_eixo_reg_nordeste = spark.sql("SELECT EIXO_TECNOLOGICO, COUNT(CODIGO_CURSO) AS QTD_CURSOS FROM GOLD.CURSOS_FINAL  WHERE UF IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') GROUP BY EIXO_TECNOLOGICO ORDER BY QTD_CURSOS DESC LIMIT 5")

df_distribuicao_cursos_eixo_reg_nordeste_pd = df_distribuicao_cursos_eixo_reg_nordeste.toPandas()

# Criando um gráfico de barras
fig = px.bar(
    df_distribuicao_cursos_eixo_reg_nordeste_pd,
    x='EIXO_TECNOLOGICO',
    y='QTD_CURSOS',
    labels={"EIXO_TECNOLOGICO": "Eixo Tecnológico", "QTD_CURSOS": "Quantidade de Cursos"},
    title="Ranking de Quantidade de Cursos por Eixo Tecnológico - Região Nordeste (5 maiores)"
)

# Adicionando rótulos acima de cada barra
fig.update_traces(
    text=df_distribuicao_cursos_eixo_reg_nordeste_pd['QTD_CURSOS'],
    textposition="inside",
    texttemplate='%{text}',
    textfont=dict(size=12)
)

fig.show()