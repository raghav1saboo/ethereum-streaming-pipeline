# Databricks notebook source
# DBTITLE 1,Cell 2
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import time

# 1. Security Configuration
storage_account = "blockchainlake"
storage_key = "vK0uvZrpnYR8Ph+g/VjmHkcEbdPjKY/buNsqtYBWBRvrFh6LiIoXoeR9hPE9wLFkUab220RCtoy5+AStgBOgOw=="

spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key)

# 2. Paths - Use a completely fresh checkpoint to avoid schema conflicts
checkpoint_suffix = int(time.time())
source_path = f"abfss://blockchain-raw@{storage_account}.dfs.core.windows.net/raw/"
checkpoint_path = f"/Volumes/blockchain_analytics_workspace/default/blockchain_data/checkpoints/bronze_v{checkpoint_suffix}"

print(f"Using new checkpoint: {checkpoint_path}")

# 3. Define UDF to parse entire transactions array
def parse_transactions_array(tx_array):
    """Parse an array of Python AttributeDict string representations"""
    if not tx_array:
        return None
    
    results = []
    for tx_str in tx_array:
        if not tx_str or tx_str == 'null':
            results.append(None)
            continue
        
        try:
            # Extract fields using regex patterns
            result = {}
            
            # Extract hash (handle both HexBytes and plain strings)
            hash_match = re.search(r"'hash':\s*(?:HexBytes\('([^']+)'\)|'([^']+)')", tx_str)
            result['hash'] = hash_match.group(1) or hash_match.group(2) if hash_match else None
            
            # Extract from address
            from_match = re.search(r"'from':\s*'([^']+)'", tx_str)
            result['from'] = from_match.group(1) if from_match else None
            
            # Extract to address
            to_match = re.search(r"'to':\s*(?:'([^']+)'|None)", tx_str)
            result['to'] = to_match.group(1) if to_match and to_match.group(1) else None
            
            # Extract value (numeric)
            value_match = re.search(r"'value':\s*(\d+)", tx_str)
            result['value'] = str(value_match.group(1)) if value_match else "0"
            
            # Extract gas
            gas_match = re.search(r"'gas':\s*(\d+)", tx_str)
            result['gas'] = str(gas_match.group(1)) if gas_match else None
            
            # Extract gasPrice (handle both gasPrice and maxFeePerGas)
            gas_price_match = re.search(r"'gasPrice':\s*(\d+)", tx_str)
            if not gas_price_match:
                gas_price_match = re.search(r"'maxFeePerGas':\s*(\d+)", tx_str)
            result['gasPrice'] = str(gas_price_match.group(1)) if gas_price_match else None
            
            results.append(result)
        except Exception as e:
            results.append(None)
    
    return results

# Define return schema for the UDF - array of structs
tx_array_schema = ArrayType(StructType([
    StructField("hash", StringType(), True),
    StructField("from", StringType(), True),
    StructField("to", StringType(), True),
    StructField("value", StringType(), True),
    StructField("gas", StringType(), True),
    StructField("gasPrice", StringType(), True)
]))

parse_txs_udf = udf(parse_transactions_array, tx_array_schema)

# 4. Read with Auto Loader - use addSchema to override inferred schema
df_raw = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
  .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle schema evolution gracefully
  .load(source_path))

# 5. Transform: First parse the transactions STRING as JSON array, then parse each element
df_bronze = df_raw.withColumn(
    "transactions_array",
    from_json(col("transactions"), "array<string>")  # Parse JSON string to array of strings
).withColumn(
    "transactions_parsed",
    parse_txs_udf(col("transactions_array"))  # Apply UDF to entire array
).drop("transactions", "transactions_array").withColumnRenamed("transactions_parsed", "transactions")

print("Starting stream write...")

# 6. Write to Bronze Delta Table with new checkpoint
(df_bronze.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")  # Allow schema evolution
  .outputMode("append")
  .trigger(availableNow=True)
  .toTable("blockchain_analytics_workspace.default.bronze_ethereum_blocks"))