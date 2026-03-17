"""
Row64 Warehouse Command Center - Unified Data Generator
Produces TWO files:
  warehouse_stream.csv  ~30-50K rows  all warehouse activity in one table
  predictions.csv       ~5K rows      ML sidecar output

Every row in warehouse_stream has a record_type so Row64 can filter:
  zone_telem   - zone telemetry readings (2s interval)
  order        - order lifecycle state
  pick         - individual item pick event
  pack         - pack/QC event
  scan         - dock scan (inbound/outbound)
  move         - zone-to-zone transfer

Shared columns across all record types (nulls where N/A):
  timestamp, record_type, zone_id, order_id,
  sku, qty, worker_id, robot_id,
  throughput, utilization, queue, workers, robots,
  status, priority, carrier, sla_mins_left,
  items_total, items_done, event_detail

Usage:
  python generate_unified.py                # default 30 min, 2500 orders
  python generate_unified.py --window 60    # 60-minute window
  python generate_unified.py --orders 3000  # more orders
"""

import csv, json, time, random, argparse, os, math
from datetime import datetime, timedelta

random.seed(42)

# ─── CONFIG ───

ZONES = [
    {"id": "dock-a",     "area": "receiving", "cap": 120, "next": ["stage-in"]},
    {"id": "dock-b",     "area": "receiving", "cap": 100, "next": ["stage-in"]},
    {"id": "stage-in",   "area": "staging",   "cap": 200, "next": ["aisle-1-4","aisle-5-8","aisle-9-12"]},
    {"id": "aisle-1-4",  "area": "storage",   "cap": 800, "next": ["pick-a"]},
    {"id": "aisle-5-8",  "area": "storage",   "cap": 800, "next": ["pick-a","pick-b"]},
    {"id": "aisle-9-12", "area": "storage",   "cap": 600, "next": ["pick-b"]},
    {"id": "pick-a",     "area": "picking",   "cap": 150, "next": ["pack"]},
    {"id": "pick-b",     "area": "picking",   "cap": 150, "next": ["pack"]},
    {"id": "pack",       "area": "packing",   "cap": 180, "next": ["stage-out"]},
    {"id": "stage-out",  "area": "outbound",  "cap": 200, "next": ["dock-c","dock-d"]},
    {"id": "dock-c",     "area": "shipping",  "cap": 100, "next": []},
    {"id": "dock-d",     "area": "shipping",  "cap": 100, "next": []},
]
ZONE_MAP = {z["id"]: z for z in ZONES}
PIPELINE = ["receiving","staging","storage","picking","packing","outbound","shipping"]
AREA_ZONES = {}
for z in ZONES:
    AREA_ZONES.setdefault(z["area"], []).append(z["id"])

SKUS = [f"SKU-{i:04d}" for i in range(1, 501)]
CARRIERS = ["UPS","FedEx","USPS","DHL","LTL-Conway","LTL-XPO"]
PRIORITIES = [1,1,1,1,2,2,3]
WORKERS = [f"W-{i:03d}" for i in range(100, 180)]
ROBOTS = [f"R-{i:03d}" for i in range(1, 25)]

BOTTLENECKS = [
    {"start": 8,  "end": 14, "zone": "pick-b",    "sev": 0.85},
    {"start": 20, "end": 25, "zone": "pack",       "sev": 0.78},
    {"start": 35, "end": 42, "zone": "pick-a",     "sev": 0.90},
    {"start": 50, "end": 55, "zone": "aisle-5-8",  "sev": 0.72},
]

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
clamp = lambda v, lo, hi: max(lo, min(hi, v))

# ─── EMPTY ROW TEMPLATE ───

COLUMNS = [
    "timestamp", "record_type", "zone_id", "order_id",
    "sku", "qty", "worker_id", "robot_id",
    "throughput", "utilization", "queue", "workers", "robots",
    "status", "priority", "carrier", "sla_mins_left",
    "items_total", "items_done", "event_detail",
]

def row(**kwargs):
    r = {c: "" for c in COLUMNS}
    r.update(kwargs)
    return r


# ─── ZONE TELEMETRY ───

def gen_zone_telemetry(window_min, base_time):
    rows = []
    state = {}
    for z in ZONES:
        state[z["id"]] = {
            "tp": random.randint(50, 90),
            "ut": random.uniform(40, 65),
            "q": random.randint(5, 20),
            "w": random.randint(3, 7),
            "r": random.randint(1, 4) if z["area"] in ("picking","storage") else 0,
        }

    interval_s = 2
    n = int(window_min * 60 / interval_s)

    for tick in range(n):
        ts = base_time + timedelta(seconds=tick * interval_s)
        ts_ms = int(ts.timestamp() * 1000)
        minute = tick * interval_s / 60.0

        for z in ZONES:
            s = state[z["id"]]
            bn = False
            for b in BOTTLENECKS:
                if b["zone"] == z["id"] and b["start"] <= minute <= b["end"]:
                    bn = True
                    break

            if bn:
                s["tp"] = clamp(s["tp"] + random.randint(-5, 2), 15, 50)
                s["ut"] = clamp(s["ut"] + random.uniform(0.5, 2.0), 75, 98)
                s["q"]  = clamp(s["q"]  + random.randint(0, 4), 25, 60)
            else:
                s["tp"] = clamp(s["tp"] + random.randint(-4, 5), 35, 130)
                s["ut"] = clamp(s["ut"] + random.uniform(-2.0, 2.0), 30, 78)
                s["q"]  = clamp(s["q"]  + random.randint(-3, 3), 0, 30)

            if tick % 30 == 0:
                s["w"] = clamp(s["w"] + random.choice([-1,0,0,0,1]), 2, 8)
                if z["area"] in ("picking","storage"):
                    s["r"] = clamp(s["r"] + random.choice([-1,0,0,1]), 0, 5)

            rows.append(row(
                timestamp=ts_ms, record_type="zone_telem", zone_id=z["id"],
                throughput=s["tp"], utilization=round(s["ut"], 1),
                queue=s["q"], workers=s["w"], robots=s["r"],
            ))

    return rows


# ─── ORDER + PICK + SCAN + MOVE EVENTS ───

def gen_order_events(n_orders, window_min, base_time):
    rows = []
    stage_times = [180, 120, 300, 240, 150, 180]

    for i in range(n_orders):
        oid = f"ORD-{100000 + i}"
        pri = random.choice(PRIORITIES)
        n_items = random.randint(1, 15) if pri < 3 else random.randint(5, 25)
        carrier = random.choice(CARRIERS)
        arrive_s = random.uniform(0, window_min * 60 * 0.7)
        arrive = base_time + timedelta(seconds=arrive_s)
        sla_h = {1: 8, 2: 4, 3: 2}[pri]
        sla_deadline = arrive + timedelta(hours=sla_h)
        end_time = base_time + timedelta(minutes=window_min)

        # Walk order through pipeline
        elapsed = (end_time - arrive).total_seconds()
        current_stage = 0
        cum = 0
        for si, st in enumerate(stage_times):
            cum += st * random.uniform(0.6, 1.5)
            if elapsed > cum:
                current_stage = si + 1
            else:
                break
        current_stage = min(current_stage, len(PIPELINE) - 1)
        current_area = PIPELINE[current_stage]
        current_zone = random.choice(AREA_ZONES[current_area])
        status_map = {0:"receiving",1:"staging",2:"stored",3:"picking",4:"packing",5:"staged",6:"shipped"}
        status = status_map.get(current_stage, "receiving")

        items_done = 0
        if current_stage <= 2: items_done = 0
        elif current_stage == 3: items_done = random.randint(0, n_items - 1)
        elif current_stage >= 4: items_done = n_items

        sla_left = max(0, (sla_deadline - end_time).total_seconds() / 60)

        # Order state row
        rows.append(row(
            timestamp=int(arrive.timestamp() * 1000),
            record_type="order", zone_id=current_zone, order_id=oid,
            status=status, priority=pri, carrier=carrier,
            sla_mins_left=round(sla_left, 1),
            items_total=n_items, items_done=items_done,
        ))

        # Storage and pick zone for this order
        storage_zone = random.choice(AREA_ZONES["storage"])
        pick_zone = random.choice(ZONE_MAP[storage_zone]["next"])
        order_skus = random.sample(SKUS, min(n_items, len(SKUS)))
        event_time = arrive

        # Scan events for completed stages
        for si in range(min(current_stage + 1, len(PIPELINE))):
            area = PIPELINE[si]
            zone = random.choice(AREA_ZONES[area])
            dt = stage_times[min(si, len(stage_times)-1)] * random.uniform(0.5, 1.2)
            event_time = event_time + timedelta(seconds=dt)
            wid = random.choice(WORKERS)

            if area == "receiving":
                rows.append(row(
                    timestamp=int(event_time.timestamp()*1000),
                    record_type="scan", zone_id=zone, order_id=oid,
                    worker_id=wid, status="dock_inbound",
                    items_total=n_items,
                    event_detail=json.dumps({"scan":"inbound","carrier":carrier}),
                ))
            elif area == "staging":
                rows.append(row(
                    timestamp=int(event_time.timestamp()*1000),
                    record_type="move", zone_id=zone, order_id=oid,
                    worker_id=wid, status="staged_inbound",
                    event_detail=json.dumps({"from":zone,"to":storage_zone}),
                ))
            elif area == "storage":
                rows.append(row(
                    timestamp=int(event_time.timestamp()*1000),
                    record_type="move", zone_id=storage_zone, order_id=oid,
                    worker_id=wid, status="putaway",
                    event_detail=json.dumps({"aisle":storage_zone,"bins":random.randint(1,4)}),
                ))
            elif area == "picking":
                # Pick start
                rows.append(row(
                    timestamp=int(event_time.timestamp()*1000),
                    record_type="scan", zone_id=pick_zone, order_id=oid,
                    worker_id=wid, status="pick_start",
                    items_total=n_items, items_done=0,
                ))
                # Individual item picks
                for pi in range(items_done):
                    pick_t = event_time + timedelta(seconds=random.uniform(8, 50))
                    sku = order_skus[pi] if pi < len(order_skus) else random.choice(SKUS)
                    q = random.randint(1, 6)
                    rid = random.choice(ROBOTS) if random.random() < 0.4 else ""
                    rows.append(row(
                        timestamp=int(pick_t.timestamp()*1000),
                        record_type="pick", zone_id=pick_zone, order_id=oid,
                        sku=sku, qty=q, worker_id=wid, robot_id=rid,
                        status="picked",
                        event_detail=json.dumps({"bin":f"B-{random.randint(1,200):03d}","aisle":storage_zone}),
                    ))
                    event_time = pick_t
            elif area == "packing":
                rows.append(row(
                    timestamp=int(event_time.timestamp()*1000),
                    record_type="pack", zone_id="pack", order_id=oid,
                    worker_id=wid, status="pack_complete",
                    items_total=n_items, items_done=n_items,
                    event_detail=json.dumps({"box_count":random.randint(1,4),"weight_lb":round(random.uniform(2,45),1)}),
                ))
            elif area == "outbound":
                rows.append(row(
                    timestamp=int(event_time.timestamp()*1000),
                    record_type="move", zone_id="stage-out", order_id=oid,
                    worker_id=wid, status="outbound_staged",
                    event_detail=json.dumps({"lane":random.choice(["A","B","C","D"])}),
                ))
            elif area == "shipping":
                dock = random.choice(["dock-c","dock-d"])
                rows.append(row(
                    timestamp=int(event_time.timestamp()*1000),
                    record_type="scan", zone_id=dock, order_id=oid,
                    worker_id=wid, status="ship_loaded", carrier=carrier,
                    items_total=n_items, items_done=n_items,
                    event_detail=json.dumps({"trailer":f"T-{random.randint(100,999)}","dock":dock}),
                ))

    return rows


# ─── PREDICTIONS (from zone_telem rows) ───

def gen_predictions(zone_rows):
    """Score zone_telem rows to produce predictions file."""
    rows = []
    # Group by timestamp, score every 5th unique timestamp (~5s intervals)
    by_ts = {}
    for r in zone_rows:
        by_ts.setdefault(r["timestamp"], []).append(r)

    timestamps = sorted(by_ts.keys())
    step = max(1, len(timestamps) // (len(timestamps) // 3))  # ~every 3rd reading
    if step < 2:
        step = 2

    for idx in range(0, len(timestamps), step):
        ts = timestamps[idx]
        for z in by_ts[ts]:
            u = float(z["utilization"]) / 100.0
            q = float(z["queue"]) / 50.0
            w = float(z["queue"]) / max(float(z["workers"]) * 8, 1)
            tp_gap = max(0, (70 - float(z["throughput"])) / 70.0)
            prob = clamp(
                u * 0.30 + q * 0.25 + w * 0.20 + tp_gap * 0.25 + random.uniform(-0.04, 0.04),
                0.01, 0.98
            )
            prob = round(prob, 3)
            eta = round(3 + (1.0 - prob) * 25, 1) if prob > 0.45 else ""
            sev = "critical" if prob >= 0.75 else ("elevated" if prob >= 0.50 else "normal")
            act = ""
            if prob > 0.55:
                act = json.dumps({
                    "add_workers": clamp(int(prob * 4), 1, 3),
                    "add_robots": 1 if int(z["robots"]) > 0 else 0,
                    "from_zone": "auto",
                })
            rows.append({
                "timestamp": ts,
                "zone_id": z["zone_id"],
                "bn_prob": prob,
                "eta_min": eta,
                "severity": sev,
                "action": act,
                "utilization": z["utilization"],
                "throughput": z["throughput"],
                "queue": z["queue"],
                "workers": z["workers"],
                "robots": z["robots"],
            })

    return rows


# ─── WRITE ───

def write_csv(filename, rows, fieldnames=None):
    if not rows:
        return 0
    path = os.path.join(OUT_DIR, filename)
    fn = fieldnames or (COLUMNS if "record_type" in rows[0] else list(rows[0].keys()))
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fn)
        w.writeheader()
        w.writerows(rows)
    return len(rows)


# ─── MAIN ───

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, default=30, help="Minutes (default 30)")
    ap.add_argument("--orders", type=int, default=2500, help="Orders (default 2500)")
    args = ap.parse_args()

    base = datetime.now() - timedelta(minutes=args.window)
    print(f"Window: {args.window} min | Orders: {args.orders}")
    print(f"Base: {base.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Zone telemetry
    print("Generating zone telemetry...")
    zt = gen_zone_telemetry(args.window, base)
    print(f"  {len(zt):,} zone_telem rows")

    # Order events
    print("Generating order lifecycle events...")
    oe = gen_order_events(args.orders, args.window, base)
    print(f"  {len(oe):,} order/pick/pack/scan/move rows")

    # Combine and sort by timestamp
    print("Merging and sorting...")
    all_rows = zt + oe
    all_rows.sort(key=lambda r: (r["timestamp"], r["record_type"]))

    n1 = write_csv(r"C:\Users\mikha\OneDrive\Documents\Demos\Supply Chain\warehouse_stream.csv", all_rows)
    print(f"\nwarehouse_stream.csv  {n1:>6,} rows")

    # Record type breakdown
    types = {}
    for r in all_rows:
        types[r["record_type"]] = types.get(r["record_type"], 0) + 1
    for t in sorted(types.keys()):
        print(f"  {t:14s} {types[t]:>6,}")

    print(f"\nTotal: {n1:,} rows")
    print(f"Output: {OUT_DIR}")

    print(f"\nBottleneck schedule (baked into zone_telem):")
    for b in BOTTLENECKS:
        print(f"  {b['zone']:12s} min {b['start']:>2}-{b['end']:>2}  {b['sev']:.0%}")

    s1 = os.path.getsize(os.path.join(OUT_DIR, r"C:\Users\mikha\OneDrive\Documents\Demos\Supply Chain\warehouse_stream.csv"))
    print(f"\nFile size: {s1/1024:.0f}KB")
    print(f"\nPredictions come from the ML model API (model.py serve)")
    print(f"Run: python model.py train warehouse_stream.csv")
    print(f"Then: python model.py serve")


if __name__ == "__main__":
    main()