import argparse
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
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


TARGET = "isHarmful"
DATE_COLS = ["week_start", "sample_date"]
LEAKAGE_COLS = [
    "pda",
    "potential_bloom",
]


def month_to_season(month):
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "fall"


def load_data(path):
    df = pd.read_csv(path, parse_dates=DATE_COLS)
    df = df.sort_values(["station", "week_start"]).reset_index(drop=True)

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

    drop_cols = [TARGET, *DATE_COLS, *LEAKAGE_COLS]
    X = df.drop(columns=drop_cols)
    y = df[TARGET].astype(int)

    return (
        X.loc[train_mask],
        y.loc[train_mask],
        X.loc[val_mask],
        y.loc[val_mask],
        X.loc[test_mask],
        y.loc[test_mask],
    )


def build_pipeline(X_train):
    categorical_features = [
        col for col in X_train.columns if X_train[col].dtype == "object"
    ]
    numeric_features = [
        col for col in X_train.columns if col not in categorical_features
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                SimpleImputer(strategy="median"),
                numeric_features,
            ),
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

    model = RandomForestClassifier(
        n_estimators=600,
        max_depth=8,
        min_samples_leaf=4,
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )

    return Pipeline(steps=[("preprocess", preprocessor), ("model", model)])


def tune_threshold(y_true, probabilities):
    thresholds = np.arange(0.05, 0.96, 0.01)
    scores = [
        (threshold, f1_score(y_true, probabilities >= threshold, zero_division=0))
        for threshold in thresholds
    ]
    return max(scores, key=lambda item: item[1])


def evaluate(name, y_true, probabilities, threshold):
    predictions = (probabilities >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, predictions).ravel()

    metrics = {
        "split": name,
        "threshold": threshold,
        "accuracy": accuracy_score(y_true, predictions),
        "precision": precision_score(y_true, predictions, zero_division=0),
        "recall": recall_score(y_true, predictions, zero_division=0),
        "f1": f1_score(y_true, predictions, zero_division=0),
        "roc_auc": roc_auc_score(y_true, probabilities),
        "tn": tn,
        "fp": fp,
        "fn": fn,
        "tp": tp,
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


def main():
    parser = argparse.ArgumentParser(
        description="Train a Random Forest HAB classifier from hab_ndbc_merged.csv."
    )
    parser.add_argument("--data", default="hab_ndbc_merged.csv")
    parser.add_argument("--model-out", default="rf_hab_model.pkl")
    parser.add_argument("--metrics-out", default="rf_hab_metrics.csv")
    parser.add_argument("--importance-out", default="rf_hab_feature_importance.csv")
    args = parser.parse_args()

    df = load_data(args.data)
    X_train, y_train, X_val, y_val, X_test, y_test = split_data(df)

    print("Chronological split")
    print("-" * 72)
    print_split_summary("Train <=2023", y_train)
    print_split_summary("Val 2024 H1", y_val)
    print_split_summary("Test 2024 H2+", y_test)

    pipeline = build_pipeline(X_train)
    pipeline.fit(X_train, y_train)

    val_probabilities = pipeline.predict_proba(X_val)[:, 1]
    best_threshold, best_val_f1 = tune_threshold(y_val, val_probabilities)
    print(f"\nBest validation threshold: {best_threshold:.2f} (F1={best_val_f1:.4f})")

    train_metrics = evaluate(
        "Train", y_train, pipeline.predict_proba(X_train)[:, 1], best_threshold
    )
    val_metrics = evaluate("Validation", y_val, val_probabilities, best_threshold)
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
        "dropped_columns": [*DATE_COLS, *LEAKAGE_COLS],
        "feature_columns": list(X_train.columns),
        "metrics": metrics.to_dict(orient="records"),
    }
    joblib.dump(artifact, args.model_out)

    print("\nTop 15 RF features")
    print("-" * 72)
    print(importance.head(15).to_string(index=False))
    print(f"\nSaved model: {Path(args.model_out).resolve()}")
    print(f"Saved metrics: {Path(args.metrics_out).resolve()}")
    print(f"Saved feature importance: {Path(args.importance_out).resolve()}")


if __name__ == "__main__":
    main()
