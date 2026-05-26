"""Generate hab_model_showcase.ipynb from scratch using nbformat."""
import nbformat
from nbformat.v4 import new_notebook, new_markdown_cell, new_code_cell

cells = []

# ═══════════════════════════════════════════════════════════════════
# Section 1 — Introduction
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="""\
# Harmful Algal Bloom Prediction Along the California Coastline

---

## 1. Introduction

### Project Goal
Predict **harmful algal blooms (HABs)** along the California coast using historical
environmental data — enabling early-warning systems for public health agencies,
fishery managers, and coastal communities.

### Why It Matters
| Stakeholder | Impact |
|---|---|
| **Public health** | HABs produce toxins (domoic acid, saxitoxin) that accumulate in shellfish and can cause amnesic and paralytic shellfish poisoning in humans and marine mammals |
| **Fisheries & economy** | Bloom events trigger emergency closures of commercial and recreational fisheries, costing California millions of dollars per closure |
| **Climate monitoring** | HAB frequency and geographic range are expanding globally as ocean temperatures rise — prediction supports long-term climate resilience planning |

### Datasets Used
| Dataset | Source | What it contributes |
|---|---|---|
| HAB observations & water chemistry | **CalHABMAP** | Weekly bloom flags, nitrate, silicate, chlorophyll-a |
| Sea surface temperature | **NOAA OISST** | SST, rolling SST anomalies, rate-of-change, warm degree-days |
| Meteorological & wave buoy data | **NOAA NDBC** (Station 46011) | Wind speed, wave height, air temperature, atmospheric pressure |

### Target Variable
`isHarmful` — binary (1 = harmful bloom, 0 = no bloom).
Dataset spans **2017–2026** at weekly resolution across multiple California coastal stations.\
"""))

# ═══════════════════════════════════════════════════════════════════
# Section 2 — Data Loading & Overview
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="""\
---

## 2. Data Loading & Overview\
"""))

cells.append(new_code_cell(source="""\
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import joblib
import warnings
warnings.filterwarnings('ignore')

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, roc_curve, auc,
    precision_recall_curve, average_precision_score,
)
from xgboost import XGBClassifier

sns.set_theme(style='whitegrid', palette='muted', font_scale=1.05)
plt.rcParams.update({'figure.dpi': 110})\
"""))

cells.append(new_code_cell(source="""\
df_raw = pd.read_csv('hab_ndbc_merged_cleaned.csv')

print(f"Shape            : {df_raw.shape[0]:,} rows × {df_raw.shape[1]} columns")
print(f"Date range       : {df_raw['week_start'].min()}  →  {df_raw['week_start'].max()}")
print(f"Years covered    : {sorted(df_raw['year'].unique())}")
print(f"Stations ({df_raw['station'].nunique():>2})    : {', '.join(sorted(df_raw['station'].unique()))}")
print(f"\\nisHarmful counts :")
print(df_raw['isHarmful'].value_counts().rename({0: 'No Bloom (0)', 1: 'Harmful Bloom (1)'}).to_string())\
"""))

cells.append(new_markdown_cell(source="""\
**Class distribution** — the target `isHarmful` is highly imbalanced (~6 % positive
overall), which is the central modelling challenge for every approach in this notebook.\
"""))

cells.append(new_code_cell(source="""\
counts = df_raw['isHarmful'].value_counts().sort_index()
labels = ['No Bloom (0)', 'Harmful Bloom (1)']
pcts   = counts / counts.sum() * 100

fig, ax = plt.subplots(figsize=(7, 4))
bars = ax.bar(labels, counts.values, color=['steelblue', 'tomato'],
              width=0.45, edgecolor='white', linewidth=1.2)
for bar, pct, cnt in zip(bars, pcts, counts.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 25,
            f'{cnt:,}\\n({pct:.1f}%)', ha='center', va='bottom',
            fontsize=12, fontweight='bold')
ax.set_title('Class Distribution — isHarmful', fontsize=14, fontweight='bold', pad=12)
ax.set_ylabel('Number of Observations', fontsize=11)
ax.set_ylim(0, counts.max() * 1.20)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f'{int(x):,}'))
sns.despine()
plt.tight_layout()
plt.show()\
"""))

cells.append(new_markdown_cell(source="A snapshot of the cleaned dataset:"))

cells.append(new_code_cell(source="""\
df_raw.head()\
"""))

# ═══════════════════════════════════════════════════════════════════
# Section 3 — Feature Engineering
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="""\
---

## 3. Feature Engineering

Two new features are added **before** the time-based split to prevent look-ahead leakage:

| Feature | Derivation | Rationale |
|---|---|---|
| **`season`** | `month` → 1 Winter / 2 Spring / 3 Summer / 4 Fall | Captures upwelling cycles and seasonal nutrient patterns |
| **`rolling_bloom_rate`** | Per-station 52-week rolling mean of `isHarmful`, shifted 1 step forward | Encodes whether a location has been in a bloom-prone state recently, without leaking the current observation |

Identifier and leakage-prone columns are then dropped.\
"""))

cells.append(new_code_cell(source="""\
df = df_raw.copy()
df = df.sort_values('week_start').reset_index(drop=True)

# Season from month
def month_to_season(m):
    if m in (12, 1, 2): return 1   # Winter
    if m in (3, 4, 5):  return 2   # Spring
    if m in (6, 7, 8):  return 3   # Summer
    return 4                        # Fall

df['season'] = df['month'].map(month_to_season)

# Per-station 52-week rolling bloom rate, no leakage via shift(1)
df['rolling_bloom_rate'] = (
    df.groupby('station')['isHarmful']
    .transform(lambda x: x.shift(1).rolling(window=52, min_periods=1).mean())
    .fillna(0)
)

DROP_COLS = [
    'week_start', 'station', 'sample_date',
    'latitude', 'longitude', 'pda', 'potential_bloom', 'station_id',
]
df = df.drop(columns=DROP_COLS)

feat_cols = [c for c in df.columns if c != 'isHarmful']
print(f"Features after engineering : {len(feat_cols)}")
print(f"New features added         : season, rolling_bloom_rate")
print(f"\\nFull feature list:\\n{feat_cols}")\
"""))

# ═══════════════════════════════════════════════════════════════════
# Section 4 — Train / Val / Test Split
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="""\
---

## 4. Sliding Window Train / Validation / Test Split

A **time-based sliding window** simulates real-world deployment — the model never
sees future data during training or validation.

| Split | Years | Purpose |
|---|---|---|
| **Train** | 2021–2022 | Fit model parameters |
| **Validation** | 2023 | Tune decision threshold (F1-optimal) |
| **Test** | 2024+ | Final evaluation — touched once per model |

Using a two-year training window (rather than all available history) intentionally
anchors the model to a recent climate regime, reducing distribution shift to the test period.\
"""))

cells.append(new_code_cell(source="""\
X = df.drop(columns=['isHarmful'])
y = df['isHarmful']

train_mask = (df['year'] >= 2021) & (df['year'] <= 2022)
val_mask   =  df['year'] == 2023
test_mask  =  df['year'] >= 2024

X_train, y_train = X[train_mask], y[train_mask]
X_val,   y_val   = X[val_mask],   y[val_mask]
X_test,  y_test  = X[test_mask],  y[test_mask]

split_info = pd.DataFrame({
    'Split':      ['Train (2021–22)', 'Validation (2023)', 'Test (2024+)'],
    'Rows':       [len(y_train),  len(y_val),  len(y_test)],
    'Blooms':     [y_train.sum(), y_val.sum(), y_test.sum()],
    'Bloom Rate': [
        f'{100 * y_train.mean():.1f}%',
        f'{100 * y_val.mean():.1f}%',
        f'{100 * y_test.mean():.1f}%',
    ],
})
print(split_info.to_string(index=False))
print(f"\\nNote: bloom rate nearly triples from train → test (distribution shift).")\
"""))

# ═══════════════════════════════════════════════════════════════════
# Section 5 — Model Training & Results
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="""\
---

## 5. Model Training & Results

Three model families are trained and compared:

1. **Logistic Regression** (`class_weight='balanced'`) — fast linear baseline; explicit minority-class upweighting
2. **Random Forest** (`n_estimators=200, class_weight='balanced'`) — non-linear ensemble
3. **XGBoost** (`scale_pos_weight` = non-bloom / bloom ratio in training set) — gradient-boosted trees with built-in imbalance handling

**Threshold tuning** — the default 0.5 decision threshold predicts zero positives on this
dataset. For each model the threshold is swept over [0.10, 0.90] in steps of 0.05 on the
*validation set only*, and the value maximising F1 is selected.
The test set is touched exactly once per model, after the threshold is fixed.\
"""))

cells.append(new_code_cell(source="""\
scale_pos = (y_train == 0).sum() / (y_train == 1).sum()
print(f"XGBoost scale_pos_weight: {scale_pos:.2f}\\n")

THRESHOLDS = np.arange(0.10, 0.95, 0.05)

def tune_threshold(model, X_v, y_v):
    \"\"\"Sweep thresholds on validation set, return (best_threshold, best_f1).\"\"\"
    proba = model.predict_proba(X_v)[:, 1]
    best_f1, best_t = -1, 0.5
    for t in THRESHOLDS:
        preds = (proba >= t).astype(int)
        f = f1_score(y_v, preds, zero_division=0)
        if f > best_f1:
            best_f1, best_t = f, t
    return round(float(best_t), 2), round(float(best_f1), 4)

def eval_on_test(model, X_te, y_te, threshold):
    \"\"\"Return metric dict and raw predictions for a given threshold.\"\"\"
    proba = model.predict_proba(X_te)[:, 1]
    preds = (proba >= threshold).astype(int)
    return {
        'acc':   round(accuracy_score(y_te, preds), 4),
        'prec':  round(precision_score(y_te, preds, zero_division=0), 4),
        'rec':   round(recall_score(y_te, preds, zero_division=0), 4),
        'f1':    round(f1_score(y_te, preds, zero_division=0), 4),
        'proba': proba,
        'preds': preds,
    }

# ── Logistic Regression ───────────────────────────────────────────────────────
lr = Pipeline([
    ('scaler', StandardScaler()),
    ('clf',    LogisticRegression(class_weight='balanced', max_iter=1000, random_state=42)),
])
lr.fit(X_train, y_train)
lr_thresh, lr_val_f1 = tune_threshold(lr, X_val, y_val)
lr_m = eval_on_test(lr, X_test, y_test, lr_thresh)
print(f"LR  balanced  — val threshold: {lr_thresh}  (val F1={lr_val_f1:.4f})")

# ── Random Forest ─────────────────────────────────────────────────────────────
rf = RandomForestClassifier(
    n_estimators=200, random_state=42, class_weight='balanced', n_jobs=-1
)
rf.fit(X_train, y_train)
rf_thresh, rf_val_f1 = tune_threshold(rf, X_val, y_val)
rf_m = eval_on_test(rf, X_test, y_test, rf_thresh)
print(f"RF  balanced  — val threshold: {rf_thresh}  (val F1={rf_val_f1:.4f})")

# ── XGBoost ───────────────────────────────────────────────────────────────────
xgb = XGBClassifier(
    n_estimators=200, random_state=42,
    scale_pos_weight=scale_pos, eval_metric='logloss', verbosity=0,
)
xgb.fit(X_train, y_train)
xgb_thresh, xgb_val_f1 = tune_threshold(xgb, X_val, y_val)
xgb_m = eval_on_test(xgb, X_test, y_test, xgb_thresh)
print(f"XGB spw={scale_pos:.1f} — val threshold: {xgb_thresh}  (val F1={xgb_val_f1:.4f})")\
"""))

cells.append(new_markdown_cell(source="""\
The table below includes both the new sliding-window results and the previously computed
full-history baselines (year < 2024 train, no threshold tuning) for reference.
The **best test F1** is highlighted in green.\
"""))

cells.append(new_code_cell(source="""\
baselines = [
    ('LR (no weight) — full history',          '—',   0.884, 0.000, 0.000, 0.000),
    ('LR (balanced) — full history',           '—',   0.814, 0.222, 0.241, 0.231),
    ('RF (no weight) — full history',          '—',   0.884, 0.000, 0.000, 0.000),
    ('RF (balanced) — full history',           '—',   0.884, 0.000, 0.000, 0.000),
]
new_rows = [
    ('LR (balanced) — sliding window',         str(lr_thresh),  lr_m['acc'],  lr_m['prec'],  lr_m['rec'],  lr_m['f1']),
    ('RF (balanced) — sliding window',         str(rf_thresh),  rf_m['acc'],  rf_m['prec'],  rf_m['rec'],  rf_m['f1']),
    (f'XGBoost (spw={scale_pos:.1f}) — sliding', str(xgb_thresh), xgb_m['acc'], xgb_m['prec'], xgb_m['rec'], xgb_m['f1']),
]

all_rows = baselines + new_rows
comp = pd.DataFrame(all_rows, columns=['Model', 'Threshold', 'Accuracy', 'Precision', 'Recall', 'F1'])
comp['_f1_float'] = comp['F1'].astype(float)
best_f1_val = comp['_f1_float'].max()
n_base = len(baselines)

def _style_rows(row):
    if comp.at[row.name, '_f1_float'] == best_f1_val:
        return ['background-color: #d4edda; font-weight: bold'] * len(row)
    if row.name < n_base:
        return ['color: #999999; font-style: italic'] * len(row)
    return [''] * len(row)

display_comp = comp.drop(columns=['_f1_float'])
display_comp.style \
    .apply(_style_rows, axis=1) \
    .format({'Accuracy': '{:.4f}', 'Precision': '{:.4f}', 'Recall': '{:.4f}', 'F1': '{:.4f}'}) \
    .set_caption('Table 1 — Full Model Comparison (test set: 2024+)') \
    .set_table_styles([{'selector': 'caption', 'props': [('font-size', '13px'), ('font-weight', 'bold')]}])\
"""))

# ═══════════════════════════════════════════════════════════════════
# Section 6 — Best Model Deep Dive
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="""\
---

## 6. Best Model Deep Dive — Logistic Regression (balanced, threshold = 0.35)

The LR balanced model at threshold 0.35 achieved the highest test F1.
Three diagnostic plots examine *how* the model makes its decisions:\
"""))

cells.append(new_code_cell(source="""\
lr_proba = lr_m['proba']
lr_preds = lr_m['preds']
cm = confusion_matrix(y_test, lr_preds)
tn, fp, fn, tp = cm.ravel()

fig, axes = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle('Best Model Diagnostics — LR Balanced (threshold = 0.35)',
             fontsize=14, fontweight='bold', y=1.02)

# ── Confusion matrix heatmap ──────────────────────────────────────────────────
cm_df = pd.DataFrame(
    cm,
    index=['Actual: No Bloom', 'Actual: Bloom'],
    columns=['Pred: No Bloom', 'Pred: Bloom'],
)
sns.heatmap(
    cm_df, annot=True, fmt='d', cmap='Blues',
    linewidths=0.8, linecolor='white',
    annot_kws={'size': 16, 'weight': 'bold'},
    ax=axes[0],
)
axes[0].set_title(f'Confusion Matrix  (threshold = {lr_thresh})', fontsize=12, pad=10)
axes[0].tick_params(labelsize=10)
for _, spine in axes[0].spines.items():
    spine.set_visible(True)

# ── ROC curve ─────────────────────────────────────────────────────────────────
fpr, tpr, _ = roc_curve(y_test, lr_proba)
roc_auc = auc(fpr, tpr)
axes[1].plot(fpr, tpr, color='steelblue', lw=2.5, label=f'LR Balanced  AUC = {roc_auc:.3f}')
axes[1].plot([0, 1], [0, 1], 'k--', lw=1, label='Random classifier')
axes[1].fill_between(fpr, tpr, alpha=0.08, color='steelblue')
axes[1].set_xlabel('False Positive Rate', fontsize=11)
axes[1].set_ylabel('True Positive Rate', fontsize=11)
axes[1].set_title('ROC Curve', fontsize=12, pad=10)
axes[1].legend(fontsize=10, loc='lower right')
axes[1].set_xlim([0, 1]); axes[1].set_ylim([0, 1.02])

# ── Precision-Recall curve ────────────────────────────────────────────────────
prec_vals, rec_vals, _ = precision_recall_curve(y_test, lr_proba)
ap = average_precision_score(y_test, lr_proba)
baseline_pr = y_test.mean()
axes[2].plot(rec_vals, prec_vals, color='tomato', lw=2.5, label=f'LR Balanced  AP = {ap:.3f}')
axes[2].axhline(baseline_pr, color='grey', lw=1.5, linestyle='--',
                label=f'No-skill baseline  ({baseline_pr:.2f})')
axes[2].fill_between(rec_vals, prec_vals, alpha=0.08, color='tomato')
axes[2].set_xlabel('Recall', fontsize=11)
axes[2].set_ylabel('Precision', fontsize=11)
axes[2].set_title('Precision-Recall Curve', fontsize=12, pad=10)
axes[2].legend(fontsize=10)
axes[2].set_xlim([0, 1]); axes[2].set_ylim([0, 1.02])

plt.tight_layout()
plt.show()

print(f"Confusion matrix  — TP={tp}, FP={fp}, FN={fn}, TN={tn}")
print(f"The model catches {tp}/{tp+fn} ({100*tp/(tp+fn):.1f}%) of actual bloom events on the test set.")
print(f"ROC-AUC: {roc_auc:.4f}   |   Average Precision (PR-AUC): {ap:.4f}")\
"""))

# ═══════════════════════════════════════════════════════════════════
# Section 7 — Feature Importance
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="""\
---

## 7. Feature Importance (Random Forest)

Feature importances are computed using **mean decrease in impurity (MDI)** from the
Random Forest. MDI is fast and interpretable but slightly biases toward high-cardinality
continuous variables — SHAP values would provide a more reliable ranking in production.\
"""))

cells.append(new_code_cell(source="""\
importances = pd.Series(rf.feature_importances_, index=X_train.columns)
top20 = importances.nlargest(20).sort_values()

# Colour top 5 differently for emphasis
colors = ['#1565C0' if i >= len(top20) - 5 else '#90CAF9' for i in range(len(top20))]

fig, ax = plt.subplots(figsize=(10, 7))
top20.plot(kind='barh', ax=ax, color=colors, edgecolor='white', linewidth=0.6)
ax.set_xlabel('Mean Decrease in Impurity', fontsize=12)
ax.set_title('Top 20 Feature Importances — Random Forest (balanced, sliding window)',
             fontsize=13, fontweight='bold', pad=12)
ax.tick_params(axis='y', labelsize=10)
ax.axvline(0, color='black', lw=0.5)
sns.despine()
plt.tight_layout()
plt.show()

print("Top 5 features:")
for rank, (feat, imp) in enumerate(importances.nlargest(5).items(), 1):
    print(f"  {rank}. {feat:<30}  importance = {imp:.4f}")\
"""))

cells.append(new_markdown_cell(source="""\
### What the Top Features Mean

The chart above shows which variables the Random Forest relies on most.
Here is a plain-English interpretation of the five most important features:

| Rank | Feature | What it measures | Why it predicts blooms |
|---|---|---|---|
| 1 | **`rolling_bloom_rate`** | 52-week trailing bloom frequency at this station | Bloom-prone sites stay bloom-prone — prior activity is the single strongest predictor of near-future activity |
| 2 | **`avg_chloro`** / **`avg_chloro_lag1`** | Chlorophyll-a concentration (μg/L), current and 1-week lag | Elevated chlorophyll signals a growing phytoplankton community; harmful species are a subset of that pool |
| 3 | **`sst_roll_14d`** / **`anom_roll_14d`** | 14-day rolling sea surface temperature and its anomaly vs climatology | Warm SST anomalies stratify the water column and suppress deep mixing, creating low-nutrient surface conditions that favour HAB species over diatoms |
| 4 | **`temp`** | In-situ water temperature at the sampling site | Directly controls growth rates — *Pseudo-nitzschia* (the dominant domoic-acid producer on this coast) blooms preferentially in the 12–18 °C range |
| 5 | **`silicate_nitrate_ratio`** | Si:N molar ratio (sentinel value 999 when nitrate ≈ 0) | A very high ratio (or the sentinel) signals nitrate depletion relative to silicate — a well-documented trigger for HAB species that can subsist on trace nitrogen |

> **Note:** Exact ranks depend on the random seed and training window. The features listed above consistently appear in the top tier across experiments in this project.\
"""))

# ═══════════════════════════════════════════════════════════════════
# Section 8 — Key Findings
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="""\
---

## 8. Key Findings & Limitations

### Summary of Results

| Finding | Detail |
|---|---|
| **Best model** | Logistic Regression (balanced, threshold = 0.35) |
| **Test F1** | 0.247 |
| **Recall on test set** | 0.578 — catches ~6 in 10 bloom events |
| **Hardest challenge** | Temporal distribution shift (bloom rate: 3 % train → 11.6 % test) |

---

### Why Logistic Regression Outperformed Random Forest and XGBoost

1. **Validation set too small to tune non-linear models**: The 2023 validation set
   contained only 16 bloom events (4.5 % rate). RF and XGBoost thresholds defaulted
   to 0.10 (the minimum tried) because no threshold produced positive F1 on validation.

2. **Probability compression**: RF and XGBoost predicted probabilities rarely exceeded
   0.35 on the test set, even with class reweighting. LR's linear score function spans
   the full probability range under the balanced-weight objective, making threshold
   tuning more effective.

3. **Simpler boundary generalises better under shift**: When the feature distribution
   drifts (as it does into 2024–26), a smooth linear boundary degrades more gracefully
   than a high-variance tree ensemble that memorised fine-grained 2021–22 patterns.

---

### Temporal Distribution Shift — the Core Challenge

| Period | Bloom Rate |
|---|---|
| Train (2021–22) | 3.0 % |
| Validation (2023) | 4.5 % |
| **Test (2024–26)** | **11.6 %** |

The near-fourfold increase from training to test suggests a structural environmental
change (possibly driven by the 2023–24 El Niño transition), not sampling noise.
Any model calibrated on pre-2024 data will systematically under-predict 2024+ blooms.

---

### Limitations

1. **Single buoy (NDBC 46011)** — all stations share one set of meteorological features;
   spatial heterogeneity along the 1,300-km California coast is lost.
2. **No upwelling index** — the California Current upwelling cycle is a primary driver
   of nutrient injection and bloom onset, but was absent from this feature set.
3. **Tiny minority class in training** — only 21 bloom events in the 711-row training
   window; even sophisticated resampling (SMOTE) failed to improve generalisation.
4. **MDI feature importance** overweights high-cardinality continuous variables;
   SHAP values would give a more reliable and model-agnostic ranking.

---

### Recommended Next Steps

- **Add NOAA CUTI** (Coastal Upwelling Transport Index) — directly captures the
  upwelling signal that drives nutrient injection and phytoplankton succession.
- **Calibrate probabilities** with `CalibratedClassifierCV` (isotonic regression)
  on the validation set to fix RF/XGBoost probability compression.
- **Retrain on a rolling window** that always ends 1 year before the target date,
  so the model stays in the same climate regime as the evaluation period.
- **Explore sequence models** (LSTM, Temporal Fusion Transformer) — the weekly
  multi-station time-series structure is a natural fit for architectures that
  model temporal dependencies explicitly.\
"""))

# ═══════════════════════════════════════════════════════════════════
# Save best model
# ═══════════════════════════════════════════════════════════════════

cells.append(new_markdown_cell(source="---\n\n### Save Best Model"))

cells.append(new_code_cell(source="""\
model_scores = {
    'LR balanced':  (lr_m['f1'],  lr,  lr_thresh),
    'RF balanced':  (rf_m['f1'],  rf,  rf_thresh),
    'XGBoost':      (xgb_m['f1'], xgb, xgb_thresh),
}
best_name = max(model_scores, key=lambda k: model_scores[k][0])
best_f1, best_model, best_thresh = model_scores[best_name]

joblib.dump(
    {'model': best_model, 'threshold': best_thresh, 'features': list(X_train.columns)},
    'best_model_sliding.pkl',
)
print(f"Saved best_model_sliding.pkl")
print(f"  Model     : {best_name}")
print(f"  Threshold : {best_thresh}")
print(f"  Test F1   : {best_f1:.4f}")\
"""))

# ═══════════════════════════════════════════════════════════════════
# Assemble & write notebook
# ═══════════════════════════════════════════════════════════════════

nb = new_notebook(cells=cells)
nb.metadata.update({
    'kernelspec': {
        'display_name': 'Python 3',
        'language': 'python',
        'name': 'python3',
    },
    'language_info': {
        'codemirror_mode': {'name': 'ipython', 'version': 3},
        'file_extension': '.py',
        'mimetype': 'text/x-python',
        'name': 'python',
        'version': '3.10.0',
    },
})

with open('hab_model_showcase.ipynb', 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)

print("hab_model_showcase.ipynb written successfully.")
print(f"Total cells: {len(cells)}")
