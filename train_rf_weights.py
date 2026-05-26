import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score,
    f1_score, confusion_matrix,
)

# ── Load & sort ───────────────────────────────────────────────────────────────
df = pd.read_csv("hab_ndbc_merged_cleaned.csv")
df = df.sort_values("week_start").reset_index(drop=True)

# ── Feature engineering ───────────────────────────────────────────────────────
def month_to_season(m):
    if m in (12, 1, 2): return 1
    if m in (3, 4, 5):  return 2
    if m in (6, 7, 8):  return 3
    return 4

df["season"] = df["month"].map(month_to_season)

df["rolling_bloom_rate"] = (
    df.groupby("station")["isHarmful"]
    .transform(lambda x: x.shift(1).rolling(window=52, min_periods=1).mean())
    .fillna(0)
)

DROP_COLS = [
    "week_start", "station", "sample_date",
    "latitude", "longitude", "pda", "potential_bloom", "station_id",
]
df = df.drop(columns=DROP_COLS)

# ── Split ─────────────────────────────────────────────────────────────────────
train_mask = (df["year"] >= 2019) & (df["year"] <= 2023)
val_mask   = (df["year"] == 2024) & (df["month"] <= 6)
test_mask  = ((df["year"] == 2024) & (df["month"] > 6)) | (df["year"] >= 2025)

X = df.drop(columns=["isHarmful"])
y = df["isHarmful"]

X_train, y_train = X[train_mask], y[train_mask]
X_val,   y_val   = X[val_mask],   y[val_mask]
X_test,  y_test  = X[test_mask],  y[test_mask]

def split_summary(name, y_s):
    n, pos = len(y_s), y_s.sum()
    print(f"  {name:<34}: {n:>5,} rows | {pos:>3} blooms ({100 * pos / n:.1f}%)")

print("Split sizes:")
split_summary("Train (2019–2023)",             y_train)
split_summary("Val   (2024 Jan–Jun)",          y_val)
split_summary("Test  (2024 Jul+ & 2025–26)",   y_test)
print()

# ── Models ────────────────────────────────────────────────────────────────────
MODELS = [
    ("RF1", {0: 1, 1: 10}),
    ("RF2", {0: 1, 1: 20}),
    ("RF3", {0: 1, 1: 30}),
    ("RF4", {0: 1, 1: 50}),
    ("RF5", "balanced_subsample"),
]

THRESHOLDS = np.arange(0.05, 0.95, 0.05)
RF_PARAMS  = dict(max_depth=6, min_samples_leaf=5, n_estimators=200, random_state=42, n_jobs=-1)

results = []

for name, cw in MODELS:
    cw_label = str(cw) if isinstance(cw, dict) else cw

    rf = RandomForestClassifier(class_weight=cw, **RF_PARAMS)
    rf.fit(X_train, y_train)

    # ── Threshold tuning on val ───────────────────────────────────────────────
    proba_val = rf.predict_proba(X_val)[:, 1]
    best_f1, best_t = -1, 0.05
    for t in THRESHOLDS:
        f = f1_score(y_val, (proba_val >= t).astype(int), zero_division=0)
        if f > best_f1:
            best_f1, best_t = f, round(float(t), 2)

    # ── Evaluate on test ──────────────────────────────────────────────────────
    proba_test = rf.predict_proba(X_test)[:, 1]
    preds      = (proba_test >= best_t).astype(int)
    cm         = confusion_matrix(y_test, preds)
    tn, fp, fn, tp = cm.ravel()

    acc  = accuracy_score(y_test, preds)
    prec = precision_score(y_test, preds, zero_division=0)
    rec  = recall_score(y_test, preds, zero_division=0)
    f1   = f1_score(y_test, preds, zero_division=0)

    results.append({
        "name": name, "cw_label": cw_label, "threshold": best_t,
        "acc": acc, "prec": prec, "rec": rec, "f1": f1,
        "tn": tn, "fp": fp, "fn": fn, "tp": tp,
        "val_f1": best_f1, "model": rf,
    })

    print(f"{'─'*60}")
    print(f"  {name}  class_weight={cw_label}")
    print(f"{'─'*60}")
    print(f"  Val prob range  : min={proba_val.min():.4f}  "
          f"mean={proba_val.mean():.4f}  max={proba_val.max():.4f}")
    print(f"  Best threshold  : {best_t}  (val F1={best_f1:.4f})")
    print(f"  Accuracy        : {acc:.4f}")
    print(f"  Precision       : {prec:.4f}")
    print(f"  Recall          : {rec:.4f}")
    print(f"  F1              : {f1:.4f}")
    print(f"  Confusion matrix (rows=actual, cols=pred):")
    print(f"    TN={tn:4d}  FP={fp:4d}")
    print(f"    FN={fn:4d}  TP={tp:4d}")
    print()

# ── Comparison table ──────────────────────────────────────────────────────────
best_f1_val  = max(r["f1"]  for r in results)
best_rec_val = max(r["rec"] for r in results)

cw_col = 24
header = (f"  {'Model':<5} {'class_weight':<{cw_col}} {'Thresh':>7} "
          f"{'Acc':>7} {'Prec':>7} {'Rec':>7} {'F1':>7}")
sep    = "─" * len(header)

print(sep)
print("  FINAL COMPARISON — Test Set (2024 Jul+ & 2025–26)")
print(sep)
print(header)
print(sep)
for r in results:
    tags = []
    if abs(r["f1"]  - best_f1_val)  < 1e-9: tags.append("best F1")
    if abs(r["rec"] - best_rec_val) < 1e-9: tags.append("best recall")
    tag_str = f"  ◄ {', '.join(tags)}" if tags else ""
    print(f"  {r['name']:<5} {r['cw_label']:<{cw_col}} {r['threshold']:>7.2f} "
          f"{r['acc']:>7.4f} {r['prec']:>7.4f} {r['rec']:>7.4f} {r['f1']:>7.4f}{tag_str}")
print(sep)

# ── Save best by F1 ───────────────────────────────────────────────────────────
best = max(results, key=lambda r: r["f1"])
joblib.dump(
    {"model": best["model"], "threshold": best["threshold"], "features": list(X_train.columns)},
    "rf_best_weight.pkl",
)
print(f"\nSaved rf_best_weight.pkl  [{best['name']}, "
      f"class_weight={best['cw_label']}, threshold={best['threshold']}, "
      f"test F1={best['f1']:.4f}]")
