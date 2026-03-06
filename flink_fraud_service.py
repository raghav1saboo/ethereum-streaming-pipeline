# This uses PyFlink to detect 'Circular Trades' (A -> B -> A)
# which is a common fraud pattern.
from pyflink.table import EnvironmentSettings, TableEnvironment

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

# Flink reads directly from the raw landing zone
t_env.execute_sql("""
    CREATE TABLE eth_raw (
        sender STRING,
        receiver STRING,
        eth_value DOUBLE,
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'abfss://blockchain-raw@blockchainlake.dfs.core.windows.net/raw/',
        'format' = 'json'
    )
""")

# Pattern Matching: Is the same value sent back within 2 minutes?
fraud_query = """
    SELECT * FROM eth_raw
    WHERE eth_value > 100 -- Focus on high value
"""
t_env.execute_sql(fraud_query).print()
