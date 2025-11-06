import pandas as pd
import numpy as np
import joblib
import os
from datetime import datetime

print("📊 Scoring new restaurant leads...")

# Check if model exists
model_path = '/home/aya/oxo-poc/models/restaurant_lead_model.pkl'
if not os.path.exists(model_path):
    print(f"❌ Model not found at {model_path}")
    print("⚠️  Please run train.py first to create the model")
    exit(1)

# Load model
model = joblib.load(model_path)
print("✅ Model loaded successfully")

# Load new leads
new_leads = pd.read_csv('restaurant_leads_synthetic.csv')
print(f"🎯 Scoring {len(new_leads)} new leads")

# Apply same feature engineering as in label.py
new_leads['has_website'] = new_leads['site_web'].notna().astype(int)
new_leads['has_linkedin'] = new_leads['dirigeant_linkedin'].notna().astype(int)
new_leads['has_email'] = new_leads['dirigeant_email'].notna().astype(int)
new_leads['has_phone'] = new_leads['dirigeant_telephone'].notna().astype(int)

new_leads['google_maps_note'] = pd.to_numeric(new_leads['google_maps_note'], errors='coerce')
new_leads['tripadvisor_note'] = pd.to_numeric(new_leads['tripadvisor_note'], errors='coerce')
new_leads['avg_rating'] = new_leads[['google_maps_note', 'tripadvisor_note']].mean(axis=1)

new_leads['google_maps_nb_avis'] = pd.to_numeric(new_leads['google_maps_nb_avis'], errors='coerce')
new_leads['tripadvisor_nb_avis'] = pd.to_numeric(new_leads['tripadvisor_nb_avis'], errors='coerce')
new_leads['total_reviews'] = new_leads['google_maps_nb_avis'].fillna(0) + new_leads['tripadvisor_nb_avis'].fillna(0)

new_leads['business_score'] = (
    new_leads['has_website'] * 0.15 +
    new_leads['has_linkedin'] * 0.08 +
    new_leads['has_email'] * 0.12 +
    new_leads['has_phone'] * 0.10 +
    (new_leads['avg_rating'].fillna(0) / 5) * 0.25 +
    (np.log1p(new_leads['total_reviews'].fillna(0)) / 5) * 0.15 +
    (new_leads['entreprise_ca_annuel'].fillna(0) / 2000000) * 0.10 +
    (new_leads['entreprise_effectif'].fillna(0) / 50) * 0.05
)

# Feature columns (must match training)
feature_columns = [
    'has_website', 'has_linkedin', 'has_email', 'has_phone',
    'avg_rating', 'total_reviews', 'entreprise_ca_annuel', 
    'entreprise_effectif', 'business_score'
]

X_new = new_leads[feature_columns].fillna(0)

# Predict scores
if hasattr(model, 'predict_proba'):  # Classification model
    scores = model.predict_proba(X_new)
    if scores.shape[1] > 1:
        lead_scores = scores[:, 1]  # Probability of class 1
    else:
        lead_scores = scores[:, 0]  # Only one class
else:  # Regression model
    lead_scores = model.predict(X_new)

new_leads['lead_score'] = lead_scores
new_leads['predicted_qualified'] = (lead_scores > 0.5).astype(int)

# Add scoring timestamp
new_leads['scored_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Save results
output_path = '/home/aya/oxo-poc/outputs/scored_leads.csv'
os.makedirs('/home/aya/oxo-poc/outputs', exist_ok=True)
new_leads.to_csv(output_path, index=False)

print(f"💾 Saved scored leads to: {output_path}")

# Show results
print(f"\n🎯 Scoring Results:")
print(f"High potential leads (score > 0.7): {(new_leads['lead_score'] > 0.7).sum()}")
print(f"Medium potential leads (0.3 < score <= 0.7): {((new_leads['lead_score'] > 0.3) & (new_leads['lead_score'] <= 0.7)).sum()}")
print(f"Low potential leads (score <= 0.3): {(new_leads['lead_score'] <= 0.3).sum()}")

print("\n🏆 Top 5 highest scoring leads:")
top_leads = new_leads.nlargest(5, 'lead_score')[['id', 'entreprise_raison_sociale', 'entreprise_ville', 'lead_score']]
print(top_leads.to_string(index=False))