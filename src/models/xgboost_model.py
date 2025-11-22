from pathlib import Path
import os
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import json

def load_cleaned_data():
    """Load the cleaned property data."""
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    path = Path(os.path.join(f"{BASE_DIR}", "data", "processed", "PROPERTY_RECORDS_Miami_FL_cleaned.csv"))
    return pd.read_csv(path)

def prepare_features(df):
    """
    Prepare features for modeling.
    Returns X (features) and y (target), and feature names.
    """
    # Make a copy
    data = df.copy()
    
    # Drop non-predictive columns
    columns_to_drop = [
        'addressLine1',  # Not predictive
        'city',          # All Miami
        'target_price',  # This is our target
        'latest_assessment_value',  # Too correlated with target
        'price_per_sqft',  # Derived from target, would cause leakage
        'latest_property_tax',  # Data leakage - tax is calculated from assessed value
    ]
    
    # Only drop columns that exist
    columns_to_drop = [col for col in columns_to_drop if col in data.columns]
    X = data.drop(columns=columns_to_drop)
    
    # Target variable
    y = data['target_price'].copy()
    
    # Handle categorical variables with label encoding
    categorical_cols = X.select_dtypes(include=['object']).columns.tolist()
    label_encoders = {}
    
    for col in categorical_cols:
        le = LabelEncoder()
        # Fill NaN with 'Unknown' before encoding
        X[col] = X[col].fillna('Unknown')
        X[col] = le.fit_transform(X[col].astype(str))
        label_encoders[col] = le
    
    # Fill any remaining NaN values with median
    numeric_cols = X.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        X[col] = X[col].fillna(X[col].median())
    
    return X, y, label_encoders, X.columns.tolist()

def filter_outliers(X, y, max_price_percentile=99):
    """Filter out extreme outliers in target price."""
    price_threshold = np.percentile(y, max_price_percentile)
    mask = y <= price_threshold
    print(f"Filtering outliers: Keeping {mask.sum()}/{len(y)} records (removed prices > ${price_threshold:,.0f})")
    return X[mask], y[mask]

def train_xgboost(X_train, y_train, X_val, y_val, n_estimators=200, max_depth=6,
                 learning_rate=0.1, subsample=0.8, colsample_bytree=0.8,
                 random_state=42, early_stopping_rounds=20):
    """Train an XGBoost model."""
    print(f"\nTraining XGBoost with {n_estimators} estimators...")
    
    model = XGBRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        random_state=random_state,
        n_jobs=-1,
        verbosity=1,
        tree_method='hist',  # Faster training
        early_stopping_rounds=early_stopping_rounds  # New API: in constructor
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=0
    )
    
    return model

def evaluate_model(model, X_test, y_test):
    """Evaluate model performance."""
    y_pred = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    
    # Calculate percentage errors
    mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
    
    metrics = {
        'MAE': mae,
        'RMSE': rmse,
        'R²': r2,
        'MAPE': mape
    }
    
    return metrics, y_pred

def get_feature_importance(model, feature_names):
    """Get and display feature importance."""
    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1]
    
    feature_importance = {}
    for i in range(min(20, len(feature_names))):  # Top 20 features
        idx = indices[i]
        feature_importance[feature_names[idx]] = float(importances[idx])
    
    return feature_importance

def save_model(model, label_encoders, feature_names, metrics, model_dir=None):
    """Save the trained model and metadata."""
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    if model_dir is None:
        model_dir = Path(os.path.join(f"{BASE_DIR}", "models", "saved"))
    else:
        model_dir = Path(model_dir)
    
    model_dir.mkdir(parents=True, exist_ok=True)
    
    # Save model
    model_path = model_dir / "xgboost_model.pkl"
    joblib.dump(model, model_path)
    print(f"\nModel saved to: {model_path}")
    
    # Save label encoders
    encoders_path = model_dir / "xgboost_encoders.pkl"
    joblib.dump(label_encoders, encoders_path)
    
    # Save metadata
    metadata = {
        'feature_names': feature_names,
        'metrics': {k: float(v) for k, v in metrics.items()},
        'model_type': 'XGBRegressor',
        'n_features': len(feature_names),
        'best_iteration': int(model.best_iteration) if hasattr(model, 'best_iteration') else None
    }
    
    metadata_path = model_dir / "xgboost_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    return model_path

def main():
    """Main training pipeline."""
    print("=" * 60)
    print("XGBoost Model Training")
    print("=" * 60)
    
    # Load data
    print("\n1. Loading cleaned data...")
    df = load_cleaned_data()
    print(f"   Loaded {len(df)} records")
    
    # Prepare features
    print("\n2. Preparing features...")
    X, y, label_encoders, feature_names = prepare_features(df)
    print(f"   Features: {len(feature_names)}")
    print(f"   Feature names: {feature_names}")
    
    # Filter outliers
    print("\n3. Filtering outliers...")
    X, y = filter_outliers(X, y, max_price_percentile=99)
    
    # Split data (train/val/test)
    print("\n4. Splitting data...")
    X_train, X_temp, y_train, y_temp = train_test_split(
        X, y, test_size=0.3, random_state=42
    )
    X_val, X_test, y_val, y_test = train_test_split(
        X_temp, y_temp, test_size=0.5, random_state=42
    )
    print(f"   Train set: {len(X_train)} samples")
    print(f"   Validation set: {len(X_val)} samples")
    print(f"   Test set: {len(X_test)} samples")
    
    # Train model
    print("\n5. Training model...")
    model = train_xgboost(
        X_train, y_train, X_val, y_val,
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        early_stopping_rounds=20
    )
    
    if hasattr(model, 'best_iteration'):
        print(f"   Best iteration: {model.best_iteration}")
    
    # Evaluate
    print("\n6. Evaluating model...")
    metrics, y_pred = evaluate_model(model, X_test, y_test)
    
    print("\n" + "=" * 60)
    print("Model Performance Metrics:")
    print("=" * 60)
    print(f"  Mean Absolute Error (MAE):     ${metrics['MAE']:,.2f}")
    print(f"  Root Mean Squared Error (RMSE): ${metrics['RMSE']:,.2f}")
    print(f"  R² Score:                      {metrics['R²']:.4f}")
    print(f"  Mean Absolute % Error (MAPE):  {metrics['MAPE']:.2f}%")
    
    # Feature importance
    print("\n7. Feature Importance (Top 10):")
    print("-" * 60)
    feature_importance = get_feature_importance(model, feature_names)
    for i, (feature, importance) in enumerate(list(feature_importance.items())[:10], 1):
        print(f"  {i:2d}. {feature:30s}: {importance:.4f}")
    
    # Save model
    print("\n8. Saving model...")
    save_model(model, label_encoders, feature_names, metrics)
    
    print("\n" + "=" * 60)
    print("Training complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()

