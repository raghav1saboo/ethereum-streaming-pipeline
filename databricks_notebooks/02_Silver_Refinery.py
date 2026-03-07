# Databricks notebook source
# DBTITLE 1,Cell 2
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Build the Silver Stream - transactions is already an array of structs
silver_df = (spark.readStream.table("blockchain_analytics_workspace.default.bronze_ethereum_blocks")
    .withColumn("tx", explode(col("transactions")))
    .select(
        col("number").cast("long").alias("block_number"),
        from_unixtime(col("timestamp").cast("long")).cast("timestamp").alias("block_time"),
        col("tx.hash").alias("tx_hash"),
        col("tx.from").alias("sender"),
        col("tx.to").alias("receiver"),
        col("tx.value").alias("raw_value")
    )
    .withColumn("eth_value", 
        (col("raw_value").cast("decimal(38,0)") / 1e18)
    )
    .withColumn("event_date", to_date(col("block_time")))
)

# Write to Silver table (Use V14 to clear the overflow state)
(silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/blockchain_analytics_workspace/default/blockchain_data/checkpoints/silver_v14")
    .partitionBy("event_date")
    .trigger(availableNow=True)
    .toTable("blockchain_analytics_workspace.default.silver_ethereum_transactions"))