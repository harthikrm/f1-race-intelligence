"""
F1 Race Intelligence — Predictive Model (Module E)
XGBoost binary classifier predicting podium probability (top 3 finish).
"""

import numpy as np
import pandas as pd
import duckdb
import xgboost as xgb
import shap
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, f1_score,
    classification_report, confusion_matrix
)
from sklearn.calibration import calibration_curve
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "processed" / "f1.db"


def build_feature_matrix(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Build the complete feature matrix for the prediction model.
    Combines features from modules A–D.
    """
    query = """
    WITH base AS (
        SELECT
            r.race_id, r.year, r.driver_id, r.constructor_id,
            r.grid, r.position, r.points,
            CASE WHEN r.position <= 3 THEN 1 ELSE 0 END AS podium_flag,
            ra.circuit_id
        FROM results r
        JOIN races ra ON r.race_id = ra.race_id
        WHERE r.grid > 0 AND r.grid <= 20
    ),
    -- Circuit affinity scores
    affinity AS (
        SELECT driver_id, circuit_id, affinity_score
        FROM driver_circuit_affinity
    ),
    -- Constructor efficiency (current season)
    efficiency AS (
        SELECT year, constructor_id, efficiency_rating
        FROM constructor_efficiency
    ),
    -- Constructor pit ranking
    pit_rank AS (
        SELECT year, constructor_id, pit_speed_rank
        FROM constructor_pit_ranking
    ),
    -- DNF rate at circuit
    dnf AS (
        SELECT driver_id, circuit_id, dnf_rate
        FROM driver_circuit_history
    ),
    -- Safety car probability per circuit
    sc AS (
        SELECT
            ra.circuit_id,
            COUNT(*) FILTER (WHERE r.status LIKE '%Safety%' OR r.status LIKE '%Collision%') * 1.0
            / COUNT(DISTINCT r.race_id) AS safety_car_prob
        FROM results r
        JOIN races ra ON r.race_id = ra.race_id
        GROUP BY ra.circuit_id
    )
    SELECT
        b.*,
        COALESCE(a.affinity_score, 50) AS affinity_score,
        COALESCE(e.efficiency_rating, 100) AS efficiency_rating,
        COALESCE(p.pit_speed_rank, 5) AS pit_speed_rank,
        COALESCE(d.dnf_rate, 0.1) AS dnf_rate_circuit,
        COALESCE(sc.safety_car_prob, 0.3) AS safety_car_prob,
        ct.circuit_type
    FROM base b
    LEFT JOIN affinity a ON b.driver_id = a.driver_id AND b.circuit_id = a.circuit_id
    LEFT JOIN efficiency e ON b.year = e.year AND b.constructor_id = e.constructor_id
    LEFT JOIN pit_rank p ON b.year = p.year AND b.constructor_id = p.constructor_id
    LEFT JOIN dnf d ON b.driver_id = d.driver_id AND b.circuit_id = d.circuit_id
    LEFT JOIN sc ON b.circuit_id = sc.circuit_id
    LEFT JOIN circuit_types ct ON b.circuit_id = ct.circuit_id
    ORDER BY b.race_id, b.grid
    """

    try:
        df = con.execute(query).fetchdf()
    except Exception:
        # Fallback: build from available tables only
        df = con.execute("""
            SELECT
                r.race_id, r.year, r.driver_id, r.constructor_id,
                r.grid, r.position, r.points,
                CASE WHEN r.position <= 3 THEN 1 ELSE 0 END AS podium_flag,
                ra.circuit_id
            FROM results r
            JOIN races ra ON r.race_id = ra.race_id
            WHERE r.grid > 0 AND r.grid <= 20
            ORDER BY r.race_id, r.grid
        """).fetchdf()

    return df


def prepare_features(df: pd.DataFrame) -> tuple:
    """
    Prepare feature matrix X and target y from raw data.
    Returns (X, y, feature_names).
    """
    feature_cols = [
        "grid", "affinity_score", "efficiency_rating", "pit_speed_rank",
        "dnf_rate_circuit", "safety_car_prob",
    ]

    # One-hot encode circuit type if available
    if "circuit_type" in df.columns:
        dummies = pd.get_dummies(df["circuit_type"], prefix="circuit_type")
        df = pd.concat([df, dummies], axis=1)
        feature_cols.extend(dummies.columns.tolist())

    # Only keep features that exist
    available = [c for c in feature_cols if c in df.columns]

    X = df[available].fillna(0).values
    y = df["podium_flag"].values

    return X, y, available


def train_model(df: pd.DataFrame) -> dict:
    """
    Train XGBoost podium predictor with chronological split.
    Train: 2010–2022, Validation: 2023, Test: 2024.
    """
    train_df = df[df["year"] <= 2022]
    val_df = df[df["year"] == 2023]
    test_df = df[df["year"] == 2024]

    X_train, y_train, feature_names = prepare_features(train_df)
    X_val, y_val, _ = prepare_features(val_df)
    X_test, y_test, _ = prepare_features(test_df)

    # Class imbalance weight
    pos_count = y_train.sum()
    neg_count = len(y_train) - pos_count
    scale_pos_weight = neg_count / max(pos_count, 1)

    model = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="auc",
        early_stopping_rounds=50,
        random_state=42,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    # Predictions
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    # Metrics
    roc_auc = roc_auc_score(y_test, y_pred_proba) if len(np.unique(y_test)) > 1 else 0
    report = classification_report(y_test, y_pred, output_dict=True)

    # SHAP
    explainer = shap.TreeExplainer(model)
    shap_values = explainer(X_test)

    # Precision@K (top 3 predicted per race)
    test_df = test_df.copy()
    test_df["pred_proba"] = y_pred_proba
    precision_at_3 = []
    for race_id in test_df["race_id"].unique():
        race = test_df[test_df["race_id"] == race_id]
        top3_pred = race.nlargest(3, "pred_proba")
        precision_at_3.append(top3_pred["podium_flag"].mean())

    return {
        "model": model,
        "feature_names": feature_names,
        "roc_auc": roc_auc,
        "classification_report": report,
        "shap_values": shap_values,
        "X_test": X_test,
        "y_test": y_test,
        "y_pred_proba": y_pred_proba,
        "test_df": test_df,
        "precision_at_3": np.mean(precision_at_3),
        "calibration": calibration_curve(y_test, y_pred_proba, n_bins=10),
    }


def predict_race(model, race_features: pd.DataFrame, feature_names: list) -> pd.DataFrame:
    """Predict podium probabilities for a specific race."""
    X = race_features[feature_names].fillna(0).values
    proba = model.predict_proba(X)[:, 1]
    race_features = race_features.copy()
    race_features["podium_probability"] = proba
    return race_features.sort_values("podium_probability", ascending=False)
