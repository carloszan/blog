+++
authors = ["Carlos Zansavio"]
title = "Diferença entre data warehouse e data lakes"
date = "2025-09-04"
description = "Qual será a diferença?"
tags = [
    "data-engineering",
]
categories = [
    "tutorial",
]
+++

Este é um conceito fundamental em arquitetura de dados. Aqui está uma análise clara das diferenças entre data warehouses e data lakes.

Basicamente, data warehouses armazenam dados estruturados. Assim, você pode construir um data warehouse com um ou vários bancos de dados SQL. Na nuvem, pode ser, por exemplo, o Google BigQuery.
Nessa abordagem, você lida principalmente com dados estruturados que foram processados ​​e transformados para uma finalidade específica. Você simplesmente acessa os bancos, obtém o que deseja e pronto.
As empresas podem criar data warehouses usando bancos de dados SQL como o Postgres ou uma solução em nuvem.

Por outro lado, data lakes armazenam arquivos de dados. Eles podem usar qualquer formato de arquivo de dados.
Um data lake pode ser implementado usando AWS S3, MinIO ou qualquer outro blob storage.
A questão aqui é quais dados podem ser salvos nesse armazenamento nesse storage e como você irá processá-los.
Um dos formatos mais usados ​​é o [Delta](https://delta.io/), que são arquivos Parquet, mas versionados.
Basicamente, você pode armazenar esses "arquivos parquet" e depois lê-los e gravá-los.
Para processar esses arquivos, você pode usar, por exemplo, o Apache Spark, mas também existem outras ferramentas.

Uma técnica não é melhor que a outra, sempre depende dos casos de uso.

Obrigado.
