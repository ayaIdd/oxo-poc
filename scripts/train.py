import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
import joblib
import os

print("🤖 Training restaurant lead scoring model...")

# Load labeled data
df = pd.read_csv('/home/aya/oxo-poc/data/labeled_restaurant_leads.csv')

# Feature columns for training
feature_columns = [
    'has_website', 'has_linkedin', 'has_email', 'has_phone',
    'avg_rating', 'total_reviews', 'entreprise_ca_annuel', 
    'entreprise_effectif', 'business_score'
]

# Handle missing values
X = df[feature_columns].fillna(0)
y = df['is_qualified_lead']

print(f"📊 Training on {len(X)} samples")
print(f"📈 Features: {', '.join(feature_columns)}")
print(f"🎯 Class distribution: {y.value_counts().to_dict()}")

# Check if we have both classes
if y.nunique() < 2:
    print("⚠️  WARNING: Only one class found in data! Using business_score as proxy.")
    # Use business_score as continuous target instead
    from sklearn.ensemble import RandomForestRegressor
    model = RandomForestRegressor(n_estimators=50, random_state=42)
    y = df['business_score']  # Use continuous score
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train regression model
    model.fit(X_train, y_train)
    
    # Evaluate regression
    y_pred = model.predict(X_test)
    from sklearn.metrics import mean_squared_error, r2_score
    print(f"✅ Regression R² Score: {r2_score(y_test, y_pred):.3f}")
    print(f"✅ Regression MSE: {mean_squared_error(y_test, y_pred):.3f}")
    
else:
    # Split data for classification
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"📚 Training set: {len(X_train)} samples")
    print(f"🧪 Test set: {len(X_test)} samples")

    # Train classification model
    model = RandomForestClassifier(
        n_estimators=50,
        max_depth=10,
        random_state=42
    )

    model.fit(X_train, y_train)

    # Evaluate
    y_pred = model.predict(X_test)
    
    # Safe probability prediction
    y_pred_proba = model.predict_proba(X_test)
    if y_pred_proba.shape[1] > 1:  # Check if we have multiple classes
        roc_auc = roc_auc_score(y_test, y_pred_proba[:, 1])
        print(f"✅ ROC AUC: {roc_auc:.3f}")
    else:
        print("ℹ️  Only one class probability available")

    print("📊 Model Performance:")
    print(f"✅ Accuracy: {model.score(X_test, y_test):.3f}")
    print("\n📋 Classification Report:")
    print(classification_report(y_test, y_pred))

# Feature importance
feature_importance = pd.DataFrame({
    'feature': feature_columns,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print("\n🎯 Feature Importance:")
print(feature_importance)

# Save model
model_path = '/home/aya/oxo-poc/models/restaurant_lead_model.pkl'
os.makedirs('/home/aya/oxo-poc/models', exist_ok=True)
joblib.dump(model, model_path)
print(f"💾 Model saved to: {model_path}")