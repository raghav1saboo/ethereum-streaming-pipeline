# Databricks notebook source
from pyspark.sql.functions import *

# 1. Load the Refined Silver Data
silver_df = spark.read.table("blockchain_analytics_workspace.default.silver_ethereum_transactions")

# 2. FEATURE 1: The "Whale Tracker" (High-Value Transfers)
# Strategy: Filter for massive moves (> 500 ETH) that impact market liquidity
gold_whale_alerts = silver_df.filter(col("eth_value") >= 500) \
    .select("block_number", "block_time", "sender", "receiver", "eth_value", "tx_hash") \
    .orderBy(col("eth_value").desc())

# 3. FEATURE 2: Network Health Metrics (Hourly Aggregates)
# Strategy: Use window functions to see gas trends and traffic spikes
gold_network_health = silver_df.groupBy(window(col("block_time"), "1 hour")) \
    .agg(
        count("tx_hash").alias("transaction_count"),
        sum("eth_value").alias("total_eth_volume"),
        avg("eth_value").alias("avg_transaction_size"),
        max("eth_value").alias("peak_transaction")
    ) \
    .select(
        col("window.start").alias("hour_start"),
        "transaction_count", 
        "total_eth_volume", 
        "avg_transaction_size"
    )

# 3. FEATURE 3: Wallet Behavior Profiling
# Strategy: Identify 'Power Users' vs. 'Dormant Wallets'
# We calculate total volume, transaction counts, and unique interactions per wallet.

gold_wallet_behavior = silver_df.groupBy("sender") \
    .agg(
        count("tx_hash").alias("total_transactions"),
        sum("eth_value").alias("total_eth_sent"),
        avg("eth_value").alias("avg_tx_value"),
        countDistinct("receiver").alias("unique_recipients"),
        min("block_time").alias("first_seen"),
        max("block_time").alias("last_active")
    ) \
    .withColumn("wallet_rank", 
        when(col("total_eth_sent") > 1000, "Institution")
        .when(col("total_eth_sent") > 100, "Whale")
        .otherwise("Retail")
    )

# Save the third view to Gold
gold_wallet_behavior.write.mode("overwrite") \
    .saveAsTable("blockchain_analytics_workspace.default.gold_wallet_behavior")

# 4. Save to Gold Tables (Overwrite mode for daily refreshes)
gold_whale_alerts.write.mode("overwrite") \
    .saveAsTable("blockchain_analytics_workspace.default.gold_whale_alerts")

gold_network_health.write.mode("overwrite") \
    .saveAsTable("blockchain_analytics_workspace.default.gold_network_health")

print("✅ Gold Analytics Tables Refreshed successfully.")