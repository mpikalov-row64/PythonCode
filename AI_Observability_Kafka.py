import pandas as pd
import random
import time
import uuid
from datetime import datetime, timedelta
import numpy as np
from kafka import KafkaProducer
import json

# =========================================================
# CONFIGURATION

INPUT_CSV = r"C:\Users\mikha\OneDrive\Documents\Demos\AI Observability\OpenAI_MockResponses.csv"
NUM_MODELS = [
    "gpt-4.1",
    "gpt-4o-mini",
    "gpt-3.5-turbo",
    "llama2-13b",
    "mistral-7b-instruct"
]

LATENCY_RANGE = (0.1, 1.5)
INPUT_TOKENS_RANGE = (10, 80)
OUTPUT_TOKENS_RANGE = (30, 200)
READABILITY_RANGE = (30, 90)
COMPLEXITY_RANGE = (1, 10)
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

NORMAL_CENTER = [0.5, 0.5]
ATTACK_CENTER = [0.9, 0.9]

MAX_MESSAGES = 100_000

# Time range for timestamps: last 3 months
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=90)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")  # datetime -> str
)
TOPIC_KAFKA = "llmevents"

# Agent Steps Mapping (Mermaid Graph)
AGENT_STEPS = [
    "step_3", "step_5a", "step_5b", "step_6", "step_7", "step_8a", "step_8b"
]
STEP_NAMES = {
    "step_3": "Safety Filter (Guardrail)",
    "step_5a": "Knowledge Retrieval",
    "step_5b": "Tools / External API",
    "step_6": "LLM Inference",
    "step_7": "Hallucination Check",
    "step_8a": "Retry Request",
    "step_8b": "User Response"
}

# =========================================================
# HELPER FUNCTIONS

def mock_metrics(model_output):
    input_tokens = random.randint(*INPUT_TOKENS_RANGE)
    output_tokens = random.randint(*OUTPUT_TOKENS_RANGE)
    total_tokens = input_tokens + output_tokens
    latency = round(random.uniform(*LATENCY_RANGE), 3)
    
    num_sentences = len(model_output.split("."))
    num_words = len(model_output.split())
    
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "latency_sec": latency,
        "estimated_cost_usd": round((input_tokens/1000)*0.0002 + (output_tokens/1000)*0.0008,6),
        "num_sentences": num_sentences,
        "num_words": num_words,
        "num_special_tokens": random.randint(0,5),
        "prompt_complexity_score": random.randint(*COMPLEXITY_RANGE),
        "output_readability_score": random.randint(*READABILITY_RANGE),
        "hallucination_score": round(random.uniform(0,1),3),
        "repetition_score": round(random.uniform(0,1),3),
        "sentiment_score": round(random.uniform(-1,1),3),
        "response_variation_score": round(random.uniform(0,1),3),
        "toxicity_score": round(random.uniform(0,1),3),
        "model_version": f"v{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
        "num_edits_required": random.randint(0,5)
    }

def generate_embeddings(event_type):
    if event_type == "jailbreak_attempt":
        x, y = np.random.normal(ATTACK_CENTER, 0.05, 2)
    else:
        x, y = np.random.normal(NORMAL_CENTER, 0.2, 2)
    return float(np.clip(x,0,1)), float(np.clip(y,0,1))

def random_datetime(start, end):
    """Generate a random datetime between start and end"""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# =========================================================
# LOAD INPUT CSV

df_input = pd.read_csv(INPUT_CSV)
print(f"Loaded {len(df_input)} rows from {INPUT_CSV}")

# =========================================================
# STREAM EVENTS TO KAFKA (100k messages)

message_count = 0

while message_count < MAX_MESSAGES:
    for idx, row_input in df_input.iterrows():
        for model in NUM_MODELS:
            if message_count >= MAX_MESSAGES:
                print(f"✅ Reached {MAX_MESSAGES} messages. Stopping stream.")
                break
            
            seed_val = RANDOM_SEED + idx + hash(model) % 10000
            random.seed(seed_val)
            
            # Event type
            event_type = "jailbreak_attempt" if random.random() > 0.95 else "llm_generation"
            
            # Assign agent step
            if event_type == "jailbreak_attempt":
                agent_step_id = "step_4a"
                agent_step_name = "Blocked Request"
            else:
                agent_step_id = random.choice(AGENT_STEPS)
                agent_step_name = STEP_NAMES.get(agent_step_id, agent_step_id)
            
            embedding_x, embedding_y = generate_embeddings(event_type)
            metrics = mock_metrics(row_input["mock_response"])
            
            # Realistic timestamp
            generation_dt = random_datetime(START_DATE, END_DATE)
            
            # Build record
            record = row_input.to_dict()
            record.update({
                "trace_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{idx}-{model}")),
                "model": model,
                "event_type": event_type,
                "embedding_x": embedding_x,
                "embedding_y": embedding_y,
                "generation_timestamp": generation_dt,
                "agent_step_id": agent_step_id,
                "agent_step_name": agent_step_name
            })
            record.update(metrics)
            
            # Send to Kafka
            producer.send(TOPIC_KAFKA, record)
            
            message_count += 1
            if message_count % 1000 == 0:
                print(f"[{message_count}] messages sent...")
            
            # Small sleep to simulate near-real-time streaming
            time.sleep(0.001)
