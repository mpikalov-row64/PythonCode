"""
RUNS ON: Local machine
PURPOSE: Train bottleneck predictor, export pickle for deployment

  python train.py                          # train on warehouse_stream.csv
  python train.py --data path/to/data.csv  # train on specific file
  python train.py --out model_v2.pkl       # custom output name

Produces: bottleneck_model.pkl (deploy this to Row64 server)
"""
import argparse, pickle, os
import numpy as np
import pandas as pd

FEATURES = [
    "utilization", "throughput", "queue", "workers", "robots",
    "util_queue", "worker_load", "tp_gap", "queue_rate", "util_rate",
]

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


def add_rates(df):
    df = df.sort_values(["zone_id", "timestamp"]).reset_index(drop=True)
    df["queue_rate"] = 0.0
    df["util_rate"] = 0.0
    for zid in df["zone_id"].unique():
        idx = df[df["zone_id"] == zid].index.values
        q = df.loc[idx, "queue"].astype(float).values
        u = df.loc[idx, "utilization"].astype(float).values
        for i in range(5, len(idx)):
            df.loc[idx[i], "queue_rate"] = round(float(q[i] - q[i-5]), 2)
            df.loc[idx[i], "util_rate"] = round(float(u[i] - u[i-5]), 2)
    return df


def label_bottlenecks(df):
    df = df.sort_values(["zone_id", "timestamp"]).reset_index(drop=True)
    stressed = (
        (df["utilization"].astype(float) > 78) &
        (df["queue"].astype(float) > 25) &
        (df["throughput"].astype(float) < 55)
    ).astype(int).values

    labels = np.zeros(len(df), dtype=int)
    for zid in df["zone_id"].unique():
        idx = df[df["zone_id"] == zid].index.values
        s = stressed[idx]
        for i in range(len(s)):
            if i >= 2 and s[i] and s[i-1] and s[i-2]:
                labels[idx[i]] = 1
            window = s[i:i+15]
            for j in range(len(window) - 2):
                if window[j] and window[j+1] and window[j+2]:
                    labels[idx[i]] = 1
                    break
    return labels


def train(data_path, out_path):
    print(f"Loading {data_path}...")
    raw = pd.read_csv(data_path)
    t = raw[raw["record_type"] == "zone_telem"].copy()
    print(f"Telemetry rows: {len(t):,}")

    print("Computing rate features...")
    t = add_rates(t)

    print("Labeling bottlenecks from telemetry...")
    t["bottleneck"] = label_bottlenecks(t)
    pos = int(t["bottleneck"].sum())
    neg = len(t) - pos
    print(f"Labels: {pos:,} bottleneck ({pos/len(t)*100:.1f}%), {neg:,} normal")

    X = extract_features(t)
    y = t["bottleneck"]

    try:
        from xgboost import XGBClassifier
        model = XGBClassifier(
            n_estimators=150, max_depth=5, learning_rate=0.1,
            scale_pos_weight=neg / max(pos, 1), random_state=42,
        )
        mtype = "xgboost"
    except ImportError:
        from sklearn.ensemble import GradientBoostingClassifier
        model = GradientBoostingClassifier(
            n_estimators=150, max_depth=5, learning_rate=0.1, random_state=42,
        )
        mtype = "sklearn_gb"

    print(f"Training {mtype}...")
    model.fit(X, y)

    from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
    probs = model.predict_proba(X)[:, 1]
    preds = (probs > 0.5).astype(int)
    print(f"\nAccuracy: {accuracy_score(y, preds):.3f}")
    print(f"AUC:      {roc_auc_score(y, probs):.3f}")
    print(classification_report(y, preds, target_names=["normal", "bottleneck"]))

    if hasattr(model, "feature_importances_"):
        imp = sorted(zip(FEATURES, model.feature_importances_), key=lambda x: -x[1])
        print("Feature importance:")
        for name, score in imp:
            print(f"  {name:14s} {score:.3f} {'#' * int(score * 50)}")

    # Package everything the scorer needs
    bundle = {
        "model": model,
        "type": mtype,
        "features": FEATURES,
        "version": "1.0",
        "trained_on": os.path.basename(data_path),
        "rows": len(t),
        "positive_rate": round(pos / len(t), 4),
    }

    with open(out_path, "wb") as f:
        pickle.dump(bundle, f)

    size_kb = os.path.getsize(out_path) / 1024
    print(f"\nSaved {out_path} ({size_kb:.0f} KB)")
    print(f"Deploy this file to the Row64 server.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Train bottleneck predictor")
    ap.add_argument("--data", default=r"C:\Users\mikha\OneDrive\Documents\Demos\Supply Chain\warehouse_stream.csv", help="Training data path")
    ap.add_argument("--out", default=r"C:\Users\mikha\OneDrive\Documents\Demos\Supply Chain\bottleneck_model.pkl", help="Output pickle path")
    args = ap.parse_args()
    train(args.data, args.out)