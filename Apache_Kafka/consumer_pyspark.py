import os
import sys  
import json     
import time
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, sum as _sum, count, round as _round, avg, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType


def validar_java():
    """Valida se a versão do Java instalada é compatível com o PySpark 3.5.1."""
    try:
        resultado = subprocess.run(
            ["java", "-version"], capture_output=True, text=True, check=False
        )
        versao = resultado.stderr or resultado.stdout
        primeira_linha = versao.splitlines()[0] if versao else "versão desconhecida"
        versao_maior = int(primeira_linha.split('"')[1].split('.')[0])
        
        if versao_maior > 17:
            raise RuntimeError(
                f"Java incompatível ({primeira_linha}). "
                "PySpark 3.5.1 deve ser executado com Java 17 ou anterior."
            )
    except (IndexError, ValueError):
        pass

def criar_sessao_spark():
    """Inicializa a sessão do PySpark com o conector Kafka."""
    return SparkSession.builder \
        .appName("EcommerceSalesStreamingProcessor") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

def main():
    validar_java()
    spark = criar_sessao_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("⚡ Inicializando Consumer em PySpark (Structured Streaming)...")

    # Schema alinhado com o payload enviado pelo Producer
    schema_produto = StructType([
        StructField("nome_produto", StringType(), True),
        StructField("quantidade", IntegerType(), True),
        StructField("preco_unitario", DoubleType(), True),
        StructField("subtotal", DoubleType(), True)
    ])

    schema_venda = StructType([
        StructField("id_ordem", StringType(), True),
        StructField("documento_cliente", StringType(), True),
        StructField("produtos_comprados", ArrayType(schema_produto), True),
        StructField("quantidade_total_itens", IntegerType(), True),
        StructField("valor_total_venda", DoubleType(), True),
        StructField("data_hora_venda", StringType(), True)
    ])

    # 1. Leitura do Stream de dados do Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "e-commerce-vendas") \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Processamento e Parse do JSON em tempo real
    df_vendas = df_kafka.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema_venda).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp_venda", to_timestamp(col("data_hora_venda"), "dd/MM/yyyy HH:mm:ss"))

    # 3. Transformação: Explodir o array de produtos para extração individual
    df_itens_venda = df_vendas.select(
        col("id_ordem"),
        col("documento_cliente"),
        col("timestamp_venda"),
        explode(col("produtos_comprados")).alias("produto")
    ).select(
        col("id_ordem"),
        col("documento_cliente"),
        col("timestamp_venda"),
        col("produto.nome_produto").alias("nome_produto"),
        col("produto.quantidade").alias("quantidade"),
        col("produto.subtotal").alias("subtotal")
    )

    # 4. Agrupamento por Produto e Cálculo do Valor Total por Produto
    df_resumo_produtos = df_itens_venda.groupBy("nome_produto") \
        .agg(
            _sum("quantidade").alias("total_itens_vendidos"),
            _round(_sum("subtotal"), 2).alias("faturamento_total_rs"),
            count("id_ordem").alias("total_pedidos")
        ) \
        .orderBy(col("faturamento_total_rs").desc())

    # 5. Exibição contínua no console
    query = df_resumo_produtos.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", "./checkpoints/ecommerce-sales") \
        .option("truncate", "false") \
        .start()

    print("📊 Agregação em Tempo Real Iniciada! Aguardando novos dados do Kafka...\n")
    query.awaitTermination()

if __name__ == "__main__":
    main()