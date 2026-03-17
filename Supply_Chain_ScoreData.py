"""
RUNS ON: Row64 server
Reads warehouse_stream.ramdb, scores via ML model, writes predictions.ramdb.

  python scorer.py
  python scorer.py --source /var/www/ramdb/live/RAMDB.Row64/Stream/warehouse_stream.ramdb
  python scorer.py --dest /var/www/ramdb/loading/RAMDB.Row64/Predictions/predictions.ramdb
  python scorer.py --interval 5

Pickle: /opt/row64/models/bottleneck_model.pkl
"""
import os, sys, time, json, pickle, signal
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from row64tools import ramdb

# ── Defaults ──

MODEL = "/opt/row64/models/bottleneck_model.pkl"
SOURCE = "/var/www/ramdb/live/RAMDB.Row64/Stream/warehouse_stream.ramdb"
DEST = "/var/www/ramdb/loading/RAMDB.Row64/Predictions/predictions.ramdb"
INTERVAL = 5

FEATURES = [
    "utilization", "throughput", "queue", "workers", "robots",
    "util_queue", "worker_load", "tp_gap", "queue_rate", "util_rate",
]

# ── Feature extraction (must match train.py) ──

def extract_features(df):
    f = pd.DataFrame(index=df.index)
    f["utilization"] = df["utilization"].astype(float)
    f["throughput"] = df["throughput"].astype(float)
    f["queue"] = df["queue"].astype(float)
    f["workers"] = df["workers"].astype(float)
    f["robots"] = df["robots"].astype(float)
    f["util_queue"] = f["utilization"] * f["queue"] / 100.0
    f["worker_load"] = f["queue"] / (f["workers"] * 8).clip(lower=1)
    f["tp_gap"] = (80 - f["throughput"]).clip(lower=0)
    f["queue_rate"] = df["queue_rate"].astype(float) if "queue_rate" in df.columns else 0.0
    f["util_rate"] = df["util_rate"].astype(float) if "util_rate" in df.columns else 0.0
    return f[FEATURES]


def score_cycle(model_data, source, dest, history):
    """
    One scoring cycle:
      1. Read warehouse_stream.ramdb from source
      2. Filter zone_telem, get latest per zone
      3. Compute rate features from recent history
      4. Run predict_proba
      5. Write predictions.ramdb to dest
    """

    if not os.path.exists(source):
        return history, None

    # Read input
    raw = ramdb.load_to_df(source)
    telem = raw[raw["record_type"] == "zone_telem"].copy()
    if len(telem) == 0:
        return history, None

    telem["timestamp"] = telem["timestamp"].astype(int)

    # Update per-zone history for rate computation
    for zid in telem["zone_id"].unique():
        z_rows = telem[telem["zone_id"] == zid].sort_values("timestamp").tail(30)
        history[zid] = z_rows

    # Latest reading per zone
    latest = telem.groupby("zone_id").tail(1).copy()
    latest["queue_rate"] = 0.0
    latest["util_rate"] = 0.0

    # Rates from history
    for zid in latest["zone_id"].values:
        if zid in history and len(history[zid]) >= 5:
            h = history[zid]
            q = h["queue"].astype(float).values
            u = h["utilization"].astype(float).values
            latest.loc[latest["zone_id"] == zid, "queue_rate"] = round(float(q[-1] - q[-5]), 2)
            latest.loc[latest["zone_id"] == zid, "util_rate"] = round(float(u[-1] - u[-5]), 2)

    # Score
    X = extract_features(latest)
    probs = model_data["model"].predict_proba(X)[:, 1]

    # Build predictions dataframe
    rows = []
    ts = int(time.time() * 1000)
    for i, (_, row) in enumerate(latest.iterrows()):
        p = round(float(probs[i]), 4)
        rows.append({
            "scored_at": ts,
            "zone_id": row["zone_id"],
            "bn_prob": p,
            "eta_min": round(3 + (1.0 - p) * 25, 1) if p > 0.40 else 0.0,
            "severity": "critical" if p >= 0.70 else ("elevated" if p >= 0.45 else "normal"),
            "action": json.dumps({
                "add_workers": min(3, max(1, int(p * 4))),
                "add_robots": 1 if float(row.get("robots", 0)) > 0 else 0,
                "from_zone": "auto",
            }) if p > 0.50 else "",
            "utilization": float(row["utilization"]),
            "throughput": float(row["throughput"]),
            "queue": float(row["queue"]),
            "workers": float(row["workers"]),
            "robots": float(row["robots"]),
        })

    pred_df = pd.DataFrame(rows)

    # Write output
    Path(os.path.dirname(dest)).mkdir(parents=True, exist_ok=True)
    ramdb.save_from_df(pred_df, dest)

    return history, pred_df


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Row64 bottleneck scorer")
    ap.add_argument("--model", default=MODEL, help="Path to pickled model")
    ap.add_argument("--source", default=SOURCE, help="Input: warehouse_stream.ramdb path")
    ap.add_argument("--dest", default=DEST, help="Output: predictions.ramdb path")
    ap.add_argument("--interval", type=float, default=INTERVAL, help="Seconds between cycles")
    args = ap.parse_args()

    # Load model
    print(f"Model:    {args.model}")
    with open(args.model, "rb") as f:
        md = pickle.load(f)
    print(f"  Type:   {md['type']}")
    print(f"  Rows:   {md.get('rows', '?')}")
    print(f"Source:   {args.source}")
    print(f"Dest:     {args.dest}")
    print(f"Interval: {args.interval}s")
    print()

    # Ensure output folder exists
    Path(os.path.dirname(args.dest)).mkdir(parents=True, exist_ok=True)

    # Graceful shutdown
    running = [True]
    def stop(sig, frame):
        print("\nShutting down...")
        running[0] = False
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    # Main loop
    history = {}
    cycle = 0
    print("Scoring started.")

    while running[0]:
        try:
            history, pred_df = score_cycle(md, args.source, args.dest, history)

            if pred_df is not None:
                hi = pred_df[pred_df["bn_prob"] > 0.50]
                alerts = ", ".join(
                    f"{r['zone_id']}={r['bn_prob']:.0%}" for _, r in hi.iterrows()
                ) or "clear"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] cycle={cycle} zones={len(pred_df)} alerts=[{alerts}]")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] cycle={cycle} waiting...")

            cycle += 1

        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] error: {e}")

        time.sleep(args.interval)

    print("Scorer stopped.")


if __name__ == "__main__":
    main()