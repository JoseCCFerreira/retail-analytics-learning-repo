from __future__ import annotations

import json

import numpy as np
from sklearn.metrics import accuracy_score, mean_absolute_error, r2_score

from deep_learning.common_retail_dl import (
    OUTPUT_DIR,
    build_classification_dataset,
    build_regression_dataset,
)

try:
    import tensorflow as tf
    from tensorflow import keras
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "TensorFlow is not installed. Run: pip install -r requirements_deep_learning.txt"
    ) from exc


def build_regression_model(input_dim: int) -> keras.Model:
    model = keras.Sequential(
        [
            keras.layers.Input(shape=(input_dim,)),
            keras.layers.Dense(32, activation="relu"),
            keras.layers.Dense(16, activation="relu"),
            keras.layers.Dense(1),
        ]
    )
    model.compile(optimizer="adam", loss="mse", metrics=["mae"])
    return model


def build_classification_model(input_dim: int, output_dim: int) -> keras.Model:
    model = keras.Sequential(
        [
            keras.layers.Input(shape=(input_dim,)),
            keras.layers.Dense(32, activation="relu"),
            keras.layers.Dropout(0.1),
            keras.layers.Dense(16, activation="relu"),
            keras.layers.Dense(output_dim, activation="softmax"),
        ]
    )
    model.compile(
        optimizer="adam",
        loss="sparse_categorical_crossentropy",
        metrics=["accuracy"],
    )
    return model


def run_regression() -> dict[str, float]:
    _, feature_cols, _, (X_train, X_test, y_train, y_test) = build_regression_dataset()
    model = build_regression_model(len(feature_cols))
    model.fit(X_train, y_train, epochs=50, batch_size=128, verbose=0, validation_split=0.1)
    preds = model.predict(X_test, verbose=0).ravel()

    model.save(OUTPUT_DIR / "tensorflow_regression_model.keras")
    return {
        "mae": round(float(mean_absolute_error(y_test, preds)), 4),
        "r2": round(float(r2_score(y_test, preds)), 4),
    }


def run_classification() -> dict[str, float]:
    _, feature_cols, segment_encoder, _, (X_train, X_test, y_train, y_test) = build_classification_dataset()
    model = build_classification_model(len(feature_cols), len(segment_encoder.classes_))
    model.fit(X_train, y_train, epochs=60, batch_size=64, verbose=0, validation_split=0.1)
    preds = np.argmax(model.predict(X_test, verbose=0), axis=1)

    model.save(OUTPUT_DIR / "tensorflow_classification_model.keras")
    return {
        "accuracy": round(float(accuracy_score(y_test, preds)), 4),
        "classes": list(segment_encoder.classes_),
    }


def main() -> None:
    tf.random.set_seed(42)
    np.random.seed(42)

    results = {
        "regression": run_regression(),
        "classification": run_classification(),
    }
    output_path = OUTPUT_DIR / "tensorflow_metrics.json"
    output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"TensorFlow metrics saved to {output_path}")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
