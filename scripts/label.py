import pandas as pd
import numpy as np
from datetime import datetime
import os

print("🚀 Starting restaurant lead labeling...")

# Load your synthetic data
df = pd.read_csv('restaurant_leads_synthetic.csv')
print(f"📊 Loaded {len(df)} restaurant leads")

# Feature Engineering
df['has_website'] = df['site_web'].notna().astype(int)
df['has_linkedin'] = df['dirigeant_linkedin'].notna().astype(int)
df['has_email'] = df['dirigeant_email'].notna().astype(int)
df['has_phone'] = df['dirigeant_telephone'].notna().astype(int)

# Calculate overall rating
df['google_maps_note'] = pd.to_numeric(df['google_maps_note'], errors='coerce')
df['tripadvisor_note'] = pd.to_numeric(df['tripadvisor_note'], errors='coerce')
df['avg_rating'] = df[['google_maps_note', 'tripadvisor_note']].mean(axis=1)

# Calculate total reviews
df['google_maps_nb_avis'] = pd.to_numeric(df['google_maps_nb_avis'], errors='coerce')
df['tripadvisor_nb_avis'] = pd.to_numeric(df['tripadvisor_nb_avis'], errors='coerce')
df['total_reviews'] = df['google_maps_nb_avis'].fillna(0) + df['tripadvisor_nb_avis'].fillna(0)

# Better business score with more realistic weights
df['business_score'] = (
    df['has_website'] * 0.15 +
    df['has_linkedin'] * 0.08 +
    df['has_email'] * 0.12 +
    df['has_phone'] * 0.10 +
    (df['avg_rating'].fillna(0) / 5) * 0.25 +
    (np.log1p(df['total_reviews'].fillna(0)) / 5) * 0.15 +  # Log scale for reviews
    (df['entreprise_ca_annuel'].fillna(0) / 2000000) * 0.10 +
    (df['entreprise_effectif'].fillna(0) / 50) * 0.05
)

# Better labeling logic - only top 30% are qualified
threshold = df['business_score'].quantile(0.7)  # Top 30%
df['is_qualified_lead'] = (df['business_score'] > threshold).astype(int)

print(f"✅ Labeled {len(df)} restaurant leads")
print(f"Qualified leads: {df['is_qualified_lead'].sum()} ({df['is_qualified_lead'].mean():.1%})")
print(f"📊 Qualification threshold: {threshold:.3f}")

# Save labeled data
output_path = '/home/aya/oxo-poc/data/labeled_restaurant_leads.csv'
os.makedirs('/home/aya/oxo-poc/data', exist_ok=True)
df.to_csv(output_path, index=False)
print(f"📁 Saved labeled data to: {output_path}")

print("\n📈 Business Score Statistics:")
print(f"Min: {df['business_score'].min():.3f}")
print(f"Max: {df['business_score'].max():.3f}")
print(f"Mean: {df['business_score'].mean():.3f}")
print(f"Std: {df['business_score'].std():.3f}")