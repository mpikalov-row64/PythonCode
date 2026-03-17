import pandas as pd
import random
import time
import uuid
from datetime import datetime, timedelta
import numpy as np
from kafka import KafkaProducer
import json
import shutil

# =========================================================
# CONFIGURATION

INPUT_CSV = "/home/row64/Downloads/OpenAI_MockResponses.csv"

LIVE_RAMDB = "/var/www/ramdb/loading/RAMDB.Row64/Examples/AIObservability.ramdb"
SEED_RAMDB = "/home/row64/Downloads/AIObservabilityRamdDB.ramdb"
RESET_AT = 50_000

NUM_MODELS = [
    "gpt-4.1",
    "gpt-4o-mini",
    "gpt-3.5-turbo",
    "llama2-13b",
    "mistral-7b-instruct"
]

MAX_MESSAGES = 100_000
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

NORMAL_CENTER = [0.5, 0.5]
ATTACK_CENTER = [0.9, 0.9]

# Time range
END_DATE = datetime.now()

# initial run
# START_DATE = END_DATE - timedelta(days=90)

# subsequent runs
START_DATE = datetime(2025, 10, 1, 0, 0, 0)

# Traces per day variability
MIN_TRACES_PER_DAY = 20
MAX_TRACES_PER_DAY = 250

# =========================================================
# KAFKA PRODUCER

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)
TOPIC_KAFKA = "llmevents"

# Agent steps
AGENT_STEPS = ["step_3", "step_5a", "step_5b", "step_6", "step_7", "step_8a", "step_8b"]
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
# HELPERS

def mock_metrics(model_output):
    input_tokens = random.randint(10, 80)
    output_tokens = random.randint(30, 200)
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
        "latency_sec": round(random.uniform(0.1, 1.5), 3),
        "estimated_cost_usd": round((input_tokens / 1000) * 0.0002 + (output_tokens / 1000) * 0.0008, 6),
        "num_words": len(model_output.split()),
        "hallucination_score": round(random.uniform(0, 1), 3),
        "toxicity_score": round(random.uniform(0, 1), 3)
    }

def generate_embeddings(event_type):
    center = ATTACK_CENTER if event_type == "jailbreak_attempt" else NORMAL_CENTER
    x, y = np.random.normal(center, 0.15, 2)
    return float(np.clip(x, 0, 1)), float(np.clip(y, 0, 1))

def maybe_reset_ramdb(message_count):
    if message_count == RESET_AT:
        shutil.copyfile(SEED_RAMDB, LIVE_RAMDB)
        print("♻️ RAMDB swapped with 100-row seed")

# =========================================================
# LOAD INPUT PROMPTS

rows = pd.read_csv(INPUT_CSV).to_dict(orient="records")
print(f"Loaded {len(rows)} prompt rows")

# =========================================================
# GENERATE DATES SCHEDULE

current_date = START_DATE.date()
all_dates = []
while current_date <= END_DATE.date():
    traces_today = random.randint(MIN_TRACES_PER_DAY, MAX_TRACES_PER_DAY)
    for _ in range(traces_today):
        # Add small random hour/minute to avoid exact duplication
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        ts = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=hour, minutes=minute, seconds=second)
        all_dates.append(ts)
    current_date += timedelta(days=1)

# Limit to MAX_MESSAGES
all_dates = all_dates[:MAX_MESSAGES]

# =========================================================
# STREAM EVENTS

message_count = 0

for ts in all_dates:
    row_input = random.choice(rows)
    model = random.choice(NUM_MODELS)
    event_type = "jailbreak_attempt" if random.random() > 0.95 else "llm_generation"
    agent_step_id = "step_4a" if event_type == "jailbreak_attempt" else random.choice(AGENT_STEPS)
    agent_step_name = "Blocked Request" if event_type == "jailbreak_attempt" else STEP_NAMES[agent_step_id]

    embedding_x, embedding_y = generate_embeddings(event_type)
    record = row_input.copy()
    record.update({
        "message_id": message_count,
        "trace_id": str(uuid.uuid4()),
        "model": model,
        "event_type": event_type,
        "embedding_x": embedding_x,
        "embedding_y": embedding_y,
        "generation_timestamp": ts,
        "agent_step_id": agent_step_id,
        "agent_step_name": agent_step_name
    })
    record.update(mock_metrics(row_input["mock_response"]))

    producer.send(TOPIC_KAFKA, record)

    message_count += 1
    maybe_reset_ramdb(message_count)

    if message_count % 1000 == 0:
        print(f"[{message_count}] messages sent")

    time.sleep(0.05)

print(f"✅ Finished sending {message_count} events")
