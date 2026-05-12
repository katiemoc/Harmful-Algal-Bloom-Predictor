import argparse
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from xgboost import XGBClassifier


TARGET = "isHarmful"
DATE_COLS = ["week_start", "sample_date"]
LEAKAGE_COLS = ["pDA", "pda", "potential_bloom"]
TARGET_RECALL = 0.80
TARGET_PRECISION = 0.30
TARGET_F1 = 0.30


def month_to_season(month):
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "fall"


def load_data(path):
    columns = pd.read_csv(path, nrows=0).columns
    df = pd.read_csv(path, parse_dates=[col for col in DATE_COLS if col in columns])
    df = df.sort_values(["station", "week_start"]).reset_index(drop=True)

    if "season" not in df.columns and "month" in df.columns:
        df["season"] = df["month"].map(month_to_season)

    df["rolling_bloom_rate_52w"] = (
        df.groupby("station")[TARGET]
        .transform(lambda s: s.shift(1).rolling(window=52, min_periods=1).mean())
        .fillna(0)
    )

    return df.sort_values("week_start").reset_index(drop=True)


def split_data(df):
    train_mask = df["year"] <= 2023
    val_mask = (df["year"] == 2024) & (df["month"] <= 6)
    test_mask = ((df["year"] == 2024) & (df["month"] > 6)) | (df["year"] >= 2025)

    drop_cols = [col for col in [TARGET, *DATE_COLS, *LEAKAGE_COLS] if col in df.columns]
    X = df.drop(columns=drop_cols)
    y = df[TARGET].astype(int)

    return (
        X.loc[train_mask],
        y.loc[train_mask],
        X.loc[val_mask],
        y.loc[val_mask],
        X.loc[test_mask],
        y.loc[test_mask],
        drop_cols,
    )


def build_pipeline(X_train, params):
    categorical_features = [
        col for col in X_train.columns if X_train[col].dtype == "object"
    ]
    numeric_features = [
        col for col in X_train.columns if col not in categorical_features
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), numeric_features),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        (
                            "onehot",
                            OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                        ),
                    ]
                ),
                categorical_features,
            ),
        ]
    )

    model = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
        **params,
    )

    return Pipeline(steps=[("preprocess", preprocessor), ("model", model)])


def candidate_params(y_train):
    negatives = int((y_train == 0).sum())
    positives = int((y_train == 1).sum())
    base_scale_pos_weight = negatives / positives

    configs = []
    for multiplier in (0.75, 1.0, 1.5, 2.0, 3.0):
        for max_depth in (2, 3, 4):
            configs.append(
                {
                    "n_estimators": 450,
                    "learning_rate": 0.03,
                    "max_depth": max_depth,
                    "min_child_weight": 3,
                    "subsample": 0.85,
                    "colsample_bytree": 0.85,
                    "reg_lambda": 5.0,
                    "reg_alpha": 0.2,
                    "scale_pos_weight": base_scale_pos_weight * multiplier,
                }
            )
    return configs


def threshold_metrics(y_true, probabilities, threshold):
    predictions = (probabilities >= threshold).astype(int)
    return {
        "threshold": float(threshold),
        "precision": precision_score(y_true, predictions, zero_division=0),
        "recall": recall_score(y_true, predictions, zero_division=0),
        "f1": f1_score(y_true, predictions, zero_division=0),
    }


def tune_threshold(y_true, probabilities):
    rows = [
        threshold_metrics(y_true, probabilities, threshold)
        for threshold in np.arange(0.01, 0.96, 0.01)
    ]

    viable = [
        row
        for row in rows
        if row["recall"] >= TARGET_RECALL
        and row["precision"] >= TARGET_PRECISION
        and row["f1"] >= TARGET_F1
    ]
    if viable:
        return max(viable, key=lambda row: (row["f1"], row["precision"], row["recall"])), True

    # When precision and F1 targets are not attainable on a small validation split,
    # keep the operating point recall-forward and then choose the cleanest threshold.
    best_recall = max(row["recall"] for row in rows)
    recall_forward = [
        row for row in rows if np.isclose(row["recall"], best_recall)
    ]
    if best_recall >= TARGET_RECALL:
        return max(
            recall_forward,
            key=lambda row: (row["f1"], row["precision"], row["threshold"]),
        ), False

    def target_distance(row):
        recall_gap = max(0, TARGET_RECALL - row["recall"])
        precision_gap = max(0, TARGET_PRECISION - row["precision"])
        f1_gap = max(0, TARGET_F1 - row["f1"])
        return recall_gap * 2.0 + precision_gap + f1_gap

    return min(rows, key=lambda row: (target_distance(row), -row["f1"])), False


def evaluate(name, y_true, probabilities, threshold):
    predictions = (probabilities >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, predictions).ravel()

    metrics = {
        "split": name,
        "threshold": float(threshold),
        "accuracy": accuracy_score(y_true, predictions),
        "precision": precision_score(y_true, predictions, zero_division=0),
        "recall": recall_score(y_true, predictions, zero_division=0),
        "f1": f1_score(y_true, predictions, zero_division=0),
        "roc_auc": roc_auc_score(y_true, probabilities),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
    }

    print(f"\n{name} metrics")
    print("-" * 72)
    print(f"threshold : {threshold:.2f}")
    print(f"accuracy  : {metrics['accuracy']:.4f}")
    print(f"precision : {metrics['precision']:.4f}")
    print(f"recall    : {metrics['recall']:.4f}")
    print(f"f1        : {metrics['f1']:.4f}")
    print(f"roc_auc   : {metrics['roc_auc']:.4f}")
    print("confusion matrix rows=actual cols=predicted")
    print(f"TN={tn:4d}  FP={fp:4d}")
    print(f"FN={fn:4d}  TP={tp:4d}")
    print(classification_report(y_true, predictions, zero_division=0))

    return metrics


def save_feature_importance(pipeline, output_path):
    preprocessor = pipeline.named_steps["preprocess"]
    model = pipeline.named_steps["model"]
    feature_names = preprocessor.get_feature_names_out()

    importance = (
        pd.DataFrame(
            {
                "feature": feature_names,
                "importance": model.feature_importances_,
            }
        )
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    importance.to_csv(output_path, index=False)
    return importance


def print_split_summary(name, y):
    positives = int(y.sum())
    rate = positives / len(y) if len(y) else 0
    print(f"{name:<16} {len(y):>5,} rows | {positives:>3} harmful ({rate:.1%})")


def model_selection_score(result):
    metrics = result["threshold_metrics"]
    misses_target = not result["target_met"]
    recall_gap = max(0, TARGET_RECALL - metrics["recall"])
    precision_gap = max(0, TARGET_PRECISION - metrics["precision"])
    f1_gap = max(0, TARGET_F1 - metrics["f1"])
    return (
        misses_target,
        recall_gap,
        -metrics["recall"],
        precision_gap + f1_gap,
        -metrics["f1"],
        -metrics["precision"],
    )


def main():
    parser = argparse.ArgumentParser(
        description="Train an XGBoost HAB classifier tuned for harmful bloom recall."
    )
    parser.add_argument("--data", default="hab_ndbc_merged.csv")
    parser.add_argument("--model-out", default="xgb_hab_model.pkl")
    parser.add_argument("--metrics-out", default="xgb_hab_metrics.csv")
    parser.add_argument("--importance-out", default="xgb_hab_feature_importance.csv")
    args = parser.parse_args()

    df = load_data(args.data)
    X_train, y_train, X_val, y_val, X_test, y_test, drop_cols = split_data(df)

    print("Chronological split")
    print("-" * 72)
    print_split_summary("Train <=2023", y_train)
    print_split_summary("Val 2024 H1", y_val)
    print_split_summary("Test 2024 H2+", y_test)
    print("\nRemoved from features:")
    print(", ".join(drop_cols))

    results = []
    for idx, params in enumerate(candidate_params(y_train), start=1):
        pipeline = build_pipeline(X_train, params)
        pipeline.fit(X_train, y_train)

        val_probabilities = pipeline.predict_proba(X_val)[:, 1]
        threshold_row, target_met = tune_threshold(y_val, val_probabilities)
        results.append(
            {
                "idx": idx,
                "pipeline": pipeline,
                "params": params,
                "threshold_metrics": threshold_row,
                "target_met": target_met,
            }
        )
        print(
            f"candidate {idx:02d}: threshold={threshold_row['threshold']:.2f} "
            f"precision={threshold_row['precision']:.3f} "
            f"recall={threshold_row['recall']:.3f} "
            f"f1={threshold_row['f1']:.3f} "
            f"target_met={target_met}"
        )

    best = min(results, key=model_selection_score)
    pipeline = best["pipeline"]
    best_threshold = best["threshold_metrics"]["threshold"]

    print("\nSelected XGBoost candidate")
    print("-" * 72)
    print(f"candidate: {best['idx']:02d}")
    print(f"threshold: {best_threshold:.2f}")
    print(f"target met on validation: {best['target_met']}")
    print(f"params: {best['params']}")

    train_metrics = evaluate(
        "Train", y_train, pipeline.predict_proba(X_train)[:, 1], best_threshold
    )
    val_metrics = evaluate(
        "Validation", y_val, pipeline.predict_proba(X_val)[:, 1], best_threshold
    )
    test_metrics = evaluate(
        "Test", y_test, pipeline.predict_proba(X_test)[:, 1], best_threshold
    )

    metrics = pd.DataFrame([train_metrics, val_metrics, test_metrics])
    metrics.to_csv(args.metrics_out, index=False)

    importance = save_feature_importance(pipeline, args.importance_out)

    artifact = {
        "model": pipeline,
        "threshold": float(best_threshold),
        "target": TARGET,
        "dropped_columns": drop_cols,
        "feature_columns": list(X_train.columns),
        "selection_params": best["params"],
        "validation_target_met": bool(best["target_met"]),
        "metrics": metrics.to_dict(orient="records"),
    }
    joblib.dump(artifact, args.model_out)

    forbidden_features = {"pDA", "pda", "potential_bloom"}.intersection(X_train.columns)
    if forbidden_features:
        raise ValueError(f"Forbidden leakage features still present: {forbidden_features}")

    print("\nTop 15 XGBoost features")
    print("-" * 72)
    print(importance.head(15).to_string(index=False))
    print(f"\nSaved model: {Path(args.model_out).resolve()}")
    print(f"Saved metrics: {Path(args.metrics_out).resolve()}")
    print(f"Saved feature importance: {Path(args.importance_out).resolve()}")


if __name__ == "__main__":
    main()
