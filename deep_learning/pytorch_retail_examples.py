from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from sklearn.metrics import accuracy_score, mean_absolute_error, r2_score

from deep_learning.common_retail_dl import (
    OUTPUT_DIR,
    build_classification_dataset,
    build_regression_dataset,
)

try:
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "PyTorch is not installed. Run: pip install -r requirements_deep_learning.txt"
    ) from exc


class RegressionMLP(nn.Module):
    def __init__(self, input_dim: int) -> None:
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)


class ClassificationMLP(nn.Module):
    def __init__(self, input_dim: int, output_dim: int) -> None:
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, 32),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Linear(16, output_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)


def train_regression_model(epochs: int = 60, batch_size: int = 128) -> dict[str, float]:
    _, feature_cols, _, (X_train, X_test, y_train, y_test) = build_regression_dataset()

    train_dataset = TensorDataset(
        torch.tensor(X_train, dtype=torch.float32),
        torch.tensor(y_train, dtype=torch.float32).view(-1, 1),
    )
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

    model = RegressionMLP(input_dim=len(feature_cols))
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    for _ in range(epochs):
        model.train()
        for batch_x, batch_y in train_loader:
            optimizer.zero_grad()
            predictions = model(batch_x)
            loss = criterion(predictions, batch_y)
            loss.backward()
            optimizer.step()

    model.eval()
    with torch.no_grad():
        preds = model(torch.tensor(X_test, dtype=torch.float32)).numpy().ravel()

    metrics = {
        "mae": round(float(mean_absolute_error(y_test, preds)), 4),
        "r2": round(float(r2_score(y_test, preds)), 4),
    }

    torch.save(model.state_dict(), OUTPUT_DIR / "pytorch_regression_model.pt")
    return metrics


def train_classification_model(epochs: int = 80, batch_size: int = 64) -> dict[str, float]:
    _, feature_cols, segment_encoder, _, (X_train, X_test, y_train, y_test) = build_classification_dataset()

    train_dataset = TensorDataset(
        torch.tensor(X_train, dtype=torch.float32),
        torch.tensor(y_train, dtype=torch.long),
    )
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

    model = ClassificationMLP(
        input_dim=len(feature_cols),
        output_dim=len(segment_encoder.classes_),
    )
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    for _ in range(epochs):
        model.train()
        for batch_x, batch_y in train_loader:
            optimizer.zero_grad()
            logits = model(batch_x)
            loss = criterion(logits, batch_y)
            loss.backward()
            optimizer.step()

    model.eval()
    with torch.no_grad():
        logits = model(torch.tensor(X_test, dtype=torch.float32))
        preds = torch.argmax(logits, dim=1).numpy()

    metrics = {
        "accuracy": round(float(accuracy_score(y_test, preds)), 4),
        "classes": list(segment_encoder.classes_),
    }
    torch.save(model.state_dict(), OUTPUT_DIR / "pytorch_classification_model.pt")
    return metrics


def main() -> None:
    torch.manual_seed(42)
    np.random.seed(42)

    results = {
        "regression": train_regression_model(),
        "classification": train_classification_model(),
    }

    output_path = OUTPUT_DIR / "pytorch_metrics.json"
    output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"PyTorch metrics saved to {output_path}")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
