import asyncio
import json
from web3 import Web3
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import os
import time
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
# --- CONFIGURATION ---
# Your Alchemy URL
ALCHEMY_URL = os.getenv("ALCHEMY_URL")
# Your Event Hub Connection String (with EntityPath=ethereum-trades)
AZURE_CONN_STR = os.getenv("EVENT_HUB_KEY")
EVENT_HUB_NAME = "ethereum-trades"
STATE_FILE = "last_block.txt" # Where we store our progress

w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "blockchain-raw"

# 2. Initialize the Client
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

def upload_to_lake(block_data):
    try:
        # Create a unique filename using the block number
        blob_name = f"raw/eth_block_{block_data['number']}.json"
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        serialized_data = json.dumps(block_data, default=str)
        # Upload as a JSON string
        blob_client.upload_blob(serialized_data, overwrite=True)
        print(f"✅ Block {block_data['number']} landed in the Lake.")
    except Exception as e:
        print(f"❌ Upload failed: {e}")

def get_saved_block():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return int(f.read().strip())
    return w3.eth.block_number # Default to latest if no file exists

def save_block(block_num):
    with open(STATE_FILE, "w") as f:
        f.write(str(block_num))

async def send_with_retry(producer, block, transactions):
    """
    Dynamically splits transactions into smaller batches if they exceed 1MB.
    """
    chunk_size = len(transactions)
    
    # We use a recursive-style approach to find the right chunk size
    while chunk_size > 0:
        try:
            # Try sending in current chunk size
            for i in range(0, len(transactions), chunk_size):
                chunk = transactions[i:i + chunk_size]
                payload = {
                    "block_number": block['number'],
                    "timestamp": block['timestamp'],
                    "tx_total": len(transactions),
                    "chunk_size": chunk_size,
                    "transactions": chunk
                }
                
                serialized = json.dumps(payload, default=str)
                event_data_batch = await producer.create_batch()
                event_data_batch.add(EventData(serialized))
                await producer.send_batch(event_data_batch)
            
            # If we reach here, the whole block was sent successfully
            return chunk_size
            
        except Exception as e:
            if "size limit" in str(e).lower():
                # If 1MB limit hit, cut chunk size in half and try again
                chunk_size = chunk_size // 2
                print(f"⚠️ Payload too large for Block {block['number']}. Reducing chunk size to {chunk_size}...")
                if chunk_size == 0: 
                    print("❌ Critical: Single transaction exceeds 1MB. Skipping specialized blob.")
                    break
            else:
                raise e

async def fetch_and_send():
    # Create the Azure Producer Client
    producer = EventHubProducerClient.from_connection_string(
        conn_str=AZURE_CONN_STR, 
        eventhub_name=EVENT_HUB_NAME
    )
    
    async with producer:
        print("Starting Blockchain Stream...")
        last_processed_block = get_saved_block()
        print(f"🚀 Resuming from block: {last_processed_block}")
        
        while True:
            try:
                current_block_num = w3.eth.block_number
                
                if current_block_num > last_processed_block:
                    for b_num in range(last_processed_block + 1, current_block_num + 1):
                        # Fetch block with full transactions
                        block = w3.eth.get_block(b_num, full_transactions=True)
                        upload_to_lake(dict(block))
                        transactions = [dict(tx) for tx in block['transactions']]
                        final_chunk = await send_with_retry(producer, block, transactions)
                        print(f"✅ Block {b_num} sent (Chunk Size: {final_chunk}")
                        save_block(b_num)
                        print(f"✅ Block {b_num} checkpointed.")
                        last_processed_block = b_num
         
                await asyncio.sleep(10)
                
            except Exception as e:
                print(f"🛑 Loop Error: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(fetch_and_send())
