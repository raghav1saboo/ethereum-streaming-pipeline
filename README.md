### 🚀 Real-Time Ethereum Streaming Pipeline: Medallion Architecture
A production-grade Data Engineering pipeline that ingests live Ethereum blockchain data, processes it through a Medallion Architecture (Bronze, Silver, Gold) on Databricks, and identifies "Whale" transactions in real-time.

🏗️ Architecture Overview
The pipeline follows a modern, distributed architecture designed for scalability and cost-efficiency:

Ingestion Layer (VM): A Dockerized Python producer fetches real-time block data from the Ethereum network via the Alchemy API.

Landing Zone: Raw JSON data is streamed into Azure Blob Storage.

Transformation Layer (Databricks Serverless):

Bronze: Raw ingestion using Databricks Auto Loader for schema evolution.

Silver: Data cleaning, flattening nested JSON, and handling 256-bit arithmetic overflows.

Gold: High-level analytical aggregates for "Whale" tracking and network health.

Visualization: (Planned) Power BI dashboard connected via DirectQuery.

🛠️ Tech Stack
Language: Python, PySpark, SQL

Infrastructure: Azure VM, Azure Blob Storage (ADLS Gen2)

Orchestration: Docker, Databricks Workflows

💡 Key Technical Challenges & Solutions
1. The "BigInt" Problem (Arithmetic Overflow)Ethereum values are 256-bit (Wei), which exceeds Spark's standard 64-bit Long type.Solution: I implemented a custom transformation using unhex() and DecimalType to bypass conv() overflows, ensuring $100\%$ accuracy for high-value "Whale" transactions.
2. 2. Schema Evolution in JSONBlockchain data structures can change with network upgrades.Solution: Used Databricks Auto Loader with cloudFiles.inferColumnTypes and schema_of_json to create a self-healing pipeline that adapts to upstream changes without manual code redeployment.
   3. 3. Production ReliabilityThe producer must run 24/7 on a remote VM.Solution: Containerized the producer with a Docker Restart Policy (--restart always) and managed sensitive Azure/Alchemy credentials through Environment Variables to ensure secure, uninterrupted ingestion.
      4.
      5. 🚀 How to Run
      6. 1. Clone the Repo: git clone https://github.com/raghav1saboo/ethereum-streaming-pipeline.git
         2. Setup Environment: Create a .env file with your ALCHEMY_URL and AZURE_CONNECTION_STRING.
         3. Build Producer: docker build -t eth-producer .
         4. Run Producer: docker run -d --env-file .env eth-producer
         5. Databricks: Import the notebooks into your workspace and run the 03_eth_gold table.
Processing: Apache Spark (Structured Streaming)

Security: Environment Variables, Secret Masking, Git Push Protection
