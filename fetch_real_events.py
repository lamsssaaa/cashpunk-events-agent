#!/usr/bin/env python3
"""
Fetch REAL events in Suisse Romande using Eventbrite API + Claude API with web search.
Writes results to Firestore pendingEvents for admin validation.
"""

import os
import sys
import json
import time
import re
from datetime import datetime

import requests
import anthropic
import firebase_admin
from firebase_admin import credentials, firestore

# ── Config ──────────────────────────────────────────────────────────────────

EVENTBRITE_TOKEN = os.environ.get("EVENTBRITE_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
FIREBASE_SERVICE_ACCOUNT = os.environ.get("FIREBASE_SERVICE_ACCOUNT", "")

CANTONS = {
    "Genève":    {"lat": 46.2044, "lng": 6.1432},
    "Vaud":      {"lat": 46.5197, "lng": 6.6323},
    "Fribourg":  {"lat": 46.8065, "lng": 7.1617},
    "Neuchâtel": {"lat": 46.9900, "lng": 6.9293},
    "Valais":    {"lat": 46.2294, "lng": 7.3622},
    "Jura":      {"lat": 47.3672, "lng": 7.3413},
}

CATEGORIES = [
    "Musique", "Sport", "Culture", "Gastronomie", "Nightlife",
    "Festival", "Marché", "Théâtre", "Exposition", "Communauté",
]

DATE_START = "2026-06-01T00:00:00"
DATE_END = "2027-02-28T23:59:59"

# ── Firebase init ───────────────────────────────────────────────────────────

def init_firebase():
    if not FIREBASE_SERVICE_ACCOUNT:
        print("ERROR: FIREBASE_SERVICE_ACCOUNT not set")
        sys.exit(1)
    sa = json.loads(FIREBASE_SERVICE_ACCOUNT)
    cred = credentials.Certificate(sa)
    firebase_admin.initialize_app(cred)
    return firestore.client()

# ── Eventbrite API ──────────────────────────────────────────────────────────

def fetch_eventbrite_events(canton, coords):
    """Fetch real events from Eventbrite API for a given canton."""
    if not EVENTBRITE_TOKEN:
        print(f"  [Eventbrite] Skipping — no token")
        return []

    url = "https://www.eventbriteapi.com/v3/events/search/"
    params = {
        "location.latitude": coords["lat"],
        "location.longitude": coords["lng"],
        "location.within": "25km",
        "start_date.range_start": DATE_START,
        "start_date.range_end": DATE_END,
        "expand": "venue",
        "token": EVENTBRITE_TOKEN,
    }

    events = []
    page = 1

    while True:
        params["page"] = page
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 401:
                print(f"  [Eventbrite] Invalid token (401)")
                return []
            if resp.status_code != 200:
                print(f"  [Eventbrite] HTTP {resp.status_code}")
                break

            data = resp.json()
            eb_events = data.get("events", [])

            for ev in eb_events:
                title = ev.get("name", {}).get("text", "")
                description = ev.get("description", {}).get("text", "")
                if description and len(description) > 300:
                    description = description[:297] + "..."

                start_str = ev.get("start", {}).get("local", "")
                venue = ev.get("venue", {})
                location = venue.get("name", canton)
                city = venue.get("address", {}).get("city", "")
                source_url = ev.get("url", "")

                try:
                    dt = datetime.fromisoformat(start_str)
                    date_iso = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    start_time = dt.strftime("%H:%M")
                except Exception:
                    continue

                category = guess_category(title + " " + (description or ""))

                events.append({
                    "title": title,
                    "description": description or f"Événement à {canton}",
                    "date": date_iso,
                    "startTime": start_time,
                    "location": location,
                    "locationText": city or location,
                    "canton": canton,
                    "category": category,
                    "price": "Voir événement",
                    "organizer": ev.get("organizer", {}).get("name", ""),
                    "sourceURL": source_url,
                    "source": "eventbrite",
                })

            pagination = data.get("pagination", {})
            if page >= pagination.get("page_count", 1):
                break
            page += 1
            time.sleep(0.5)

        except requests.RequestException as e:
            print(f"  [Eventbrite] Error: {e}")
            break

    print(f"  [Eventbrite] Found {len(events)} events")
    return events

# ── Claude API with web search ──────────────────────────────────────────────

def fetch_claude_events(canton):
    """Use Claude API with web search to find local events not on Eventbrite."""
    if not ANTHROPIC_API_KEY:
        print(f"  [Claude] Skipping — no API key")
        return []

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = (
        f"Recherche sur le web les vrais événements à {canton} en Suisse "
        f"entre juin 2026 et février 2027. "
        f"Cherche sur agenda.ch, sortir.ch, local.ch, eventfrog.ch, "
        f"et les sites officiels du canton. "
        f"Retourne un JSON strict (tableau) avec cette structure pour chaque événement: "
        f'{{"title": "...", "description": "...", "date": "YYYY-MM-DDTHH:MM:SS", '
        f'"startTime": "HH:mm", "location": "...", "locationText": "...", '
        f'"canton": "{canton}", "category": "...", "price": "... CHF", '
        f'"organizer": "...", "sourceURL": "..."}}. '
        f"Categories possibles: {', '.join(CATEGORIES)}. "
        f"Minimum 8 événements. Uniquement des événements RÉELS avec sources vérifiables. "
        f"Ne invente rien. Retourne UNIQUEMENT le tableau JSON, sans markdown ni texte."
    )

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=4000,
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 5,
            }],
            messages=[{"role": "user", "content": prompt}],
        )

        text = ""
        for block in response.content:
            if hasattr(block, "text"):
                text += block.text

        events = parse_json_response(text)

        for ev in events:
            ev["source"] = "claude_web_search"
            ev["canton"] = canton
            if "category" not in ev or ev["category"] not in CATEGORIES:
                ev["category"] = guess_category(
                    ev.get("title", "") + " " + ev.get("description", "")
                )

        print(f"  [Claude] Found {len(events)} events")
        return events

    except anthropic.APIError as e:
        print(f"  [Claude] API error: {e}")
        return []
    except Exception as e:
        print(f"  [Claude] Error: {e}")
        return []

# ── Helpers ─────────────────────────────────────────────────────────────────

def parse_json_response(text):
    """Extract JSON array from text that might contain markdown fences."""
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    match = re.search(r"\[[\s\S]*\]", text)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    print(f"  Failed to parse JSON: {text[:200]}")
    return []


def guess_category(text):
    """Guess event category from title/description text."""
    text_lower = text.lower()
    keywords = {
        "Musique": ["concert", "musique", "jazz", "rock", "orchestre", "dj", "rap"],
        "Sport": ["sport", "course", "marathon", "football", "hockey", "ski", "trail"],
        "Culture": ["culture", "cinéma", "film", "conférence", "lecture", "salon"],
        "Gastronomie": ["gastronomie", "cuisine", "vin", "dégustation", "food", "brunch"],
        "Nightlife": ["nightlife", "soirée", "club", "dj set", "party", "nuit"],
        "Festival": ["festival", "fête", "carnaval", "festivités"],
        "Marché": ["marché", "brocante", "vide-grenier", "artisanat", "noël"],
        "Théâtre": ["théâtre", "spectacle", "comédie", "danse", "ballet", "opéra"],
        "Exposition": ["exposition", "expo", "musée", "galerie", "art", "peinture"],
        "Communauté": ["communauté", "bénévol", "association", "rencontre", "atelier"],
    }
    for category, kws in keywords.items():
        for kw in kws:
            if kw in text_lower:
                return category
    return "Communauté"


def write_to_firestore(db, events):
    """Write events to Firestore pendingEvents collection, skipping duplicates."""
    written = 0
    skipped = 0

    for event in events:
        title = event.get("title", "").strip()
        canton = event.get("canton", "")
        if not title or not canton:
            continue

        existing = (
            db.collection("pendingEvents")
            .where("canton", "==", canton)
            .where("title", "==", title)
            .limit(1)
            .get()
        )
        if len(existing) > 0:
            skipped += 1
            continue

        date_str = event.get("date", "")
        try:
            dt = datetime.fromisoformat(date_str)
        except Exception:
            print(f"  Skipping bad date: {title} — {date_str}")
            continue

        doc_data = {
            "title": title,
            "description": event.get("description", ""),
            "canton": canton,
            "date": dt,
            "price": event.get("price", "Gratuit"),
            "location": event.get("location", canton),
            "locationText": event.get("locationText", event.get("location", canton)),
            "photoURL": None,
            "organizerName": event.get("organizer", ""),
            "dates": [],
            "submittedBy": "real-events-agent",
            "submittedByEmail": "",
            "submittedAt": firestore.SERVER_TIMESTAMP,
            "status": "pending",
            "source": event.get("source", "unknown"),
            "sourceURL": event.get("sourceURL", ""),
            "category": event.get("category", "Communauté"),
            "startTime": event.get("startTime", ""),
        }

        db.collection("pendingEvents").add(doc_data)
        written += 1

    print(f"\n  Written: {written}, Skipped (duplicates): {skipped}")
    return written

# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print(f"=== Fetching real events — {datetime.now().isoformat()} ===\n")

    db = init_firebase()
    all_events = []

    for canton, coords in CANTONS.items():
        print(f"\n--- {canton} ---")

        eb_events = fetch_eventbrite_events(canton, coords)
        all_events.extend(eb_events)

        claude_events = fetch_claude_events(canton)
        all_events.extend(claude_events)

        time.sleep(2)

    print(f"\n=== Writing {len(all_events)} events to Firestore ===")
    total = write_to_firestore(db, all_events)
    print(f"\n=== Done. {total} new events added to pendingEvents ===")


if __name__ == "__main__":
    main()
