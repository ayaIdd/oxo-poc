"""
Générateur de données synthétiques pour leads restaurants
Génère 500-1000 leads réalistes sans scoring (données brutes)
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from faker import Faker
import json

fake = Faker('fr_FR')

# Listes de données réalistes
VILLES = [
    ('Paris', '75', 'Île-de-France'),
    ('Lyon', '69', 'Auvergne-Rhône-Alpes'),
    ('Marseille', '13', "Provence-Alpes-Côte d'Azur"),
    ('Toulouse', '31', 'Occitanie'),
    ('Nice', '06', "Provence-Alpes-Côte d'Azur"),
    ('Nantes', '44', 'Pays de la Loire'),
    ('Bordeaux', '33', 'Nouvelle-Aquitaine'),
    ('Lille', '59', 'Hauts-de-France'),
    ('Rennes', '35', 'Bretagne'),
    ('Strasbourg', '67', 'Grand Est')
]

TYPES_RESTO = [
    ('Restauration traditionnelle', '5610A'),
    ('Restauration de type rapide', '5610C'),
    ('Cafétérias', '5610A'),
    ('Restauration collective', '5629A')
]

NOMS_RESTO_PREFIXES = [
    "Le Bistrot", "La Table", "Chez", "L'Auberge", "Le Comptoir",
    "La Brasserie", "Le Jardin", "L'Atelier", "La Terrasse", "Le Coin"
]

NOMS_RESTO_SUFFIXES = [
    "Gourmand", "des Saveurs", "du Chef", "Français", "de la Place",
    "du Marché", "Modern", "Bio", "du Coin", "Parisien"
]

DOMAINES_EMAIL = [
    "gmail.com", "outlook.fr", "orange.fr", "free.fr", "wanadoo.fr"
]

def generate_restaurant_name():
    """Génère un nom de restaurant réaliste"""
    if random.random() > 0.5:
        return f"{random.choice(NOMS_RESTO_PREFIXES)} {random.choice(NOMS_RESTO_SUFFIXES)}"
    else:
        return f"Restaurant {fake.last_name()}"

def generate_email(nom, prenom, raison_sociale):
    """Génère un email professionnel réaliste"""
    patterns = [
        f"{prenom.lower()}.{nom.lower()}",
        f"{prenom[0].lower()}{nom.lower()}",
        f"contact",
        f"{prenom.lower()}"
    ]
    
    pattern = random.choice(patterns)
    
    # 70% domaine propre, 30% email générique
    if random.random() > 0.3:
        domaine = raison_sociale.lower().replace(' ', '').replace("'", '')
        domaine = ''.join(c for c in domaine if c.isalnum())
        return f"{pattern}@{domaine[:15]}.fr"
    else:
        return f"{pattern}@{random.choice(DOMAINES_EMAIL)}"

def generate_phone():
    """Génère un numéro de téléphone français"""
    prefixes = ['06', '07']
    return f"+33 {random.choice(prefixes)} {random.randint(10,99)} {random.randint(10,99)} {random.randint(10,99)} {random.randint(10,99)}"

def generate_siren():
    """Génère un numéro SIREN (9 chiffres)"""
    return ''.join([str(random.randint(0, 9)) for _ in range(9)])

def generate_linkedin(nom, prenom):
    """Génère un lien LinkedIn (50% des cas)"""
    if random.random() > 0.5:
        return f"https://www.linkedin.com/in/{prenom.lower()}-{nom.lower()}-{random.randint(100,999)}"
    return None

def generate_website(raison_sociale):
    """Génère un site web (60% des cas)"""
    if random.random() > 0.4:
        domain = raison_sociale.lower().replace(' ', '').replace("'", '')
        domain = ''.join(c for c in domain if c.isalnum())
        return f"https://www.{domain[:20]}.fr"
    return None

def generate_social_media(raison_sociale):
    """Génère présence réseaux sociaux"""
    handle = raison_sociale.lower().replace(' ', '').replace("'", '')[:20]
    
    social = {}
    
    if random.random() > 0.3:
        social['facebook'] = f"https://facebook.com/{handle}"
    
    if random.random() > 0.4:
        social['instagram'] = f"@{handle}"
    
    if random.random() > 0.85:
        social['tiktok'] = f"@{handle}"
    
    return social if social else None

def generate_google_maps_data():
    """Génère données Google Maps"""
    return {
        'note': round(random.uniform(3.5, 5.0), 1),
        'nb_avis': random.randint(10, 800),
        'url': f"https://maps.google.com/place/{random.randint(1000, 9999)}"
    }

def generate_tripadvisor_data():
    """Génère données TripAdvisor (70% des restos)"""
    if random.random() > 0.3:
        return {
            'note': round(random.uniform(3.0, 5.0), 1),
            'nb_avis': random.randint(5, 500),
            'url': f"https://www.tripadvisor.fr/Restaurant_Review-{random.randint(1000, 9999)}"
        }
    return None

def generate_creation_date():
    """Génère une date de création (10 dernières années, biais vers récent)"""
    # Plus de poids sur les 3 dernières années
    if random.random() > 0.4:
        days_ago = random.randint(30, 1095)  # 1 mois à 3 ans
    else:
        days_ago = random.randint(1095, 3650)  # 3 à 10 ans
    
    date = datetime.now() - timedelta(days=days_ago)
    return date.strftime('%Y-%m-%d')

def generate_lead(lead_id):
    """Génère un lead complet"""
    
    # Info dirigeant
    genre = random.choice(['M', 'F'])
    if genre == 'M':
        prenom = fake.first_name_male()
        fonction = random.choice(['Gérant', 'Président', 'Directeur'])
    else:
        prenom = fake.first_name_female()
        fonction = random.choice(['Gérante', 'Présidente', 'Directrice'])
    
    nom = fake.last_name()
    age = random.randint(25, 55)
    
    # Info entreprise
    ville_data = random.choice(VILLES)
    ville, dept, region = ville_data
    
    raison_sociale = generate_restaurant_name()
    type_resto, code_ape = random.choice(TYPES_RESTO)
    
    siren = generate_siren()
    siret = siren + '000' + str(random.randint(10, 99))
    
    # CA: distribution réaliste (300k-2M avec biais vers milieu)
    ca = int(random.triangular(300000, 2000000, 600000))
    
    # Effectif: corrélé avec CA
    if ca < 500000:
        effectif = random.randint(5, 10)
    elif ca < 1000000:
        effectif = random.randint(8, 15)
    else:
        effectif = random.randint(12, 20)
    
    date_creation = generate_creation_date()
    
    # Contacts
    email = generate_email(nom, prenom, raison_sociale)
    telephone = generate_phone()
    linkedin = generate_linkedin(nom, prenom)
    
    # Présence digitale
    site_web = generate_website(raison_sociale)
    google_maps = generate_google_maps_data()
    tripadvisor = generate_tripadvisor_data()
    reseaux_sociaux = generate_social_media(raison_sociale)
    
    # Construire le lead
    lead = {
        'id': f"LEAD_{lead_id:04d}",
        'date_collecte': datetime.now().isoformat(),
        'dirigeant': {
            'nom': nom,
            'prenom': prenom,
            'age': age,
            'fonction': fonction,
            'email': email,
            'telephone': telephone,
            'linkedin': linkedin
        },
        'entreprise': {
            'raison_sociale': raison_sociale,
            'siren': siren,
            'siret': siret,
            'code_ape': code_ape,
            'activite': type_resto,
            'date_creation': date_creation,
            'ca_annuel': ca,
            'effectif': effectif,
            'adresse': fake.street_address(),
            'ville': ville,
            'code_postal': f"{dept}{random.randint(100, 999):03d}",
            'region': region
        },
        'presence_digitale': {
            'site_web': site_web,
            'google_maps': google_maps,
            'tripadvisor': tripadvisor,
            'reseaux_sociaux': reseaux_sociaux
        }
    }
    
    return lead

def generate_dataset(n_leads=1000):
    """Génère un dataset complet"""
    print(f"🚀 Génération de {n_leads} leads synthétiques...")
    
    leads = []
    for i in range(1, n_leads + 1):
        lead = generate_lead(i)
        leads.append(lead)
        
        if i % 100 == 0:
            print(f"   ✓ {i}/{n_leads} leads générés")
    
    print(f"✅ Génération terminée: {len(leads)} leads")
    return leads

def save_to_files(leads, base_filename='restaurant_leads'):
    """Sauvegarde dans plusieurs formats"""
    
    # JSON
    json_file = f'{base_filename}.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(leads, f, ensure_ascii=False, indent=2)
    print(f"💾 Sauvegardé: {json_file}")
    
    # CSV (format plat)
    csv_data = []
    for lead in leads:
        flat_lead = {
            'id': lead['id'],
            'date_collecte': lead['date_collecte'],
            # Dirigeant
            'dirigeant_nom': lead['dirigeant']['nom'],
            'dirigeant_prenom': lead['dirigeant']['prenom'],
            'dirigeant_age': lead['dirigeant']['age'],
            'dirigeant_fonction': lead['dirigeant']['fonction'],
            'dirigeant_email': lead['dirigeant']['email'],
            'dirigeant_telephone': lead['dirigeant']['telephone'],
            'dirigeant_linkedin': lead['dirigeant']['linkedin'],
            # Entreprise
            'entreprise_raison_sociale': lead['entreprise']['raison_sociale'],
            'entreprise_siren': lead['entreprise']['siren'],
            'entreprise_siret': lead['entreprise']['siret'],
            'entreprise_code_ape': lead['entreprise']['code_ape'],
            'entreprise_activite': lead['entreprise']['activite'],
            'entreprise_date_creation': lead['entreprise']['date_creation'],
            'entreprise_ca_annuel': lead['entreprise']['ca_annuel'],
            'entreprise_effectif': lead['entreprise']['effectif'],
            'entreprise_adresse': lead['entreprise']['adresse'],
            'entreprise_ville': lead['entreprise']['ville'],
            'entreprise_code_postal': lead['entreprise']['code_postal'],
            'entreprise_region': lead['entreprise']['region'],
            # Présence digitale
            'site_web': lead['presence_digitale']['site_web'],
            'google_maps_note': lead['presence_digitale']['google_maps']['note'] if lead['presence_digitale']['google_maps'] else None,
            'google_maps_nb_avis': lead['presence_digitale']['google_maps']['nb_avis'] if lead['presence_digitale']['google_maps'] else None,
            'tripadvisor_note': lead['presence_digitale']['tripadvisor']['note'] if lead['presence_digitale']['tripadvisor'] else None,
            'tripadvisor_nb_avis': lead['presence_digitale']['tripadvisor']['nb_avis'] if lead['presence_digitale']['tripadvisor'] else None,
        }
        csv_data.append(flat_lead)
    
    df = pd.DataFrame(csv_data)
    csv_file = f'{base_filename}.csv'
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    print(f"💾 Sauvegardé: {csv_file}")
    
    # Stats
    print(f"\n📊 Statistiques du dataset:")
    print(f"   Total leads: {len(leads)}")
    print(f"   CA moyen: {df['entreprise_ca_annuel'].mean():,.0f}€")
    print(f"   Effectif moyen: {df['entreprise_effectif'].mean():.1f}")
    print(f"   Avec site web: {df['site_web'].notna().sum()} ({df['site_web'].notna().sum()/len(df)*100:.1f}%)")
    print(f"   Avec LinkedIn: {df['dirigeant_linkedin'].notna().sum()} ({df['dirigeant_linkedin'].notna().sum()/len(df)*100:.1f}%)")
    print(f"   Avec TripAdvisor: {df['tripadvisor_note'].notna().sum()} ({df['tripadvisor_note'].notna().sum()/len(df)*100:.1f}%)")

if __name__ == "__main__":
    # Générer 1000 leads
    leads = generate_dataset(n_leads=1000)
    
    # Sauvegarder
    save_to_files(leads, 'restaurant_leads_synthetic')
    
    print("\n✅ Terminé! Fichiers prêts pour le ML.")