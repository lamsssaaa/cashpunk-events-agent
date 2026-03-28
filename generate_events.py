import os
import json
import requests
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore

cred_json = json.loads(os.environ['FIREBASE_SERVICE_ACCOUNT'])
cred = credentials.Certificate(cred_json)
firebase_admin.initialize_app(cred)
db = firestore.client()

GEMINI_KEY = os.environ['GEMINI_API_KEY']
CANTONS = ["Genève", "Vaud", "Fribourg", "Neuchâtel", "Valais", "Jura"]

def generate_events(canton):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}"
    prompt = f"""Génère 3 événements typiques dans le canton de {canton} en Suisse romande dans les 30 prochains jours. Pour chaque événement retourne un JSON avec ces champs: title, description, location, date (ISO8601), price, category (restaurant/activite/location/autre). Retourne UNIQUEMENT un tableau JSON valide sans texte avant ou après."""
    body = {"contents": [{"parts": [{"text": prompt}]}]}
    response = requests.post(url, json=body)
    data = response.json()
    try:
        text = data['candidates'][0]['content']['parts'][0]['text'].strip()
        if text.startswith('```'):
            text = text.split('```')[1]
            if text.startswith('json'):
                text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        print(f"Error for {canton}: {e} - {data}")
        return []

def save_to_firestore(events, canton):
    for event in events:
        db.collection('pendingEvents').add({
            'title': event.get('title', ''),
            'description': event.get('description', ''),
            'location': event.get('location', canton),
            'canton': canton,
            'date': event.get('date', ''),
            'price': event.get('price', 'Gratuit'),
            'category': event.get('category', 'autre'),
            'status': 'pending',
            'source': 'ai_generated',
            'submittedAt': datetime.now().isoformat(),
            'submittedBy': 'ai_agent',
            'submittedByEmail': 'ai@cashpunk.ch'
        })
        print(f"Saved: {event.get('title')} for {canton}")

for canton in CANTONS:
    print(f"Generating for {canton}...")
    events = generate_events(canton)
    if events:
        save_to_firestore(events, canton)
    print(f"Done: {len(events)} events for {canton}")

print("All done!")
