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
import traceback
from datetime import datetime

# ── Dependency checks ───────────────────────────────────────────────────────

try:
    import requests
except ImportError:
    print("ERROR: 'requests' not installed. Run: pip install requests")
    sys.exit(1)

try:
    import anthropic
except ImportError:
    print("WARNING: 'anthropic' not installed. Claude web search will be skipped.")
    anthropic = None

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    print("ERROR: 'firebase-admin' not installed. Run: pip install firebase-admin")
    sys.exit(1)

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

CANTON_DEFAULT_PHOTOS = {
    "Genève":    "https://images.unsplash.com/photo-1573108037329-37aa135a142e?w=800",
    "Vaud":      "https://images.unsplash.com/photo-1527668752968-14dc70a27c95?w=800",
    "Fribourg":  "https://images.unsplash.com/photo-1530122037265-a5f1f91d3b99?w=800",
    "Neuchâtel": "https://images.unsplash.com/photo-1580477667995-2b94f01c9516?w=800",
    "Valais":    "https://images.unsplash.com/photo-1531366936337-7c912a4589a7?w=800",
    "Jura":      "https://images.unsplash.com/photo-1506905925346-21bda4d32df4?w=800",
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
    try:
        sa = json.loads(FIREBASE_SERVICE_ACCOUNT)
        cred = credentials.Certificate(sa)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("[Firebase] Initialized successfully")
        return db
    except json.JSONDecodeError as e:
        print(f"ERROR: FIREBASE_SERVICE_ACCOUNT is not valid JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Firebase init failed: {e}")
        sys.exit(1)

# ── Eventbrite API ──────────────────────────────────────────────────────────

def fetch_eventbrite_events(canton, coords):
    """Fetch real events from Eventbrite API for a given canton."""
    if not EVENTBRITE_TOKEN:
        print(f"  [Eventbrite] Skipping — no EVENTBRITE_TOKEN set")
        return []

    url = "https://www.eventbriteapi.com/v3/events/search/"
    params = {
        "location.latitude": coords["lat"],
        "location.longitude": coords["lng"],
        "location.within": "25km",
        "start_date.range_start": DATE_START,
        "start_date.range_end": DATE_END,
        "expand": "venue,organizer",
        "token": EVENTBRITE_TOKEN,
    }

    events = []
    page = 1

    while True:
        params["page"] = page
        try:
            print(f"  [Eventbrite] Fetching page {page}...")
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code == 401:
                print(f"  [Eventbrite] Invalid token (401) — check EVENTBRITE_TOKEN secret")
                return events
            if resp.status_code == 429:
                print(f"  [Eventbrite] Rate limited (429) — waiting 10s")
                time.sleep(10)
                continue
            if resp.status_code != 200:
                print(f"  [Eventbrite] HTTP {resp.status_code}: {resp.text[:200]}")
                break

            data = resp.json()
            eb_events = data.get("events", [])
            print(f"  [Eventbrite] Page {page}: {len(eb_events)} events")

            for ev in eb_events:
                try:
                    title = ev.get("name", {}).get("text", "")
                    if not title:
                        continue

                    description = ev.get("description", {}).get("text", "")
                    if description and len(description) > 300:
                        description = description[:297] + "..."

                    start_str = ev.get("start", {}).get("local", "")
                    end_str = ev.get("end", {}).get("local", "")
                    venue = ev.get("venue") or {}
                    location = venue.get("name", canton)
                    city = venue.get("address", {}).get("city", "")
                    source_url = ev.get("url", "")

                    # Photo from logo
                    logo = ev.get("logo") or {}
                    photo_url = logo.get("url", "") or logo.get("original", {}).get("url", "")
                    if not photo_url:
                        photo_url = CANTON_DEFAULT_PHOTOS.get(canton, "")

                    # Parse dates
                    try:
                        start_dt = datetime.fromisoformat(start_str)
                        date_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
                        start_time = start_dt.strftime("%H:%M")
                    except Exception:
                        continue

                    end_date_iso = None
                    is_multi_day = False
                    if end_str:
                        try:
                            end_dt = datetime.fromisoformat(end_str)
                            if end_dt.date() > start_dt.date():
                                end_date_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
                                is_multi_day = True
                        except Exception:
                            pass

                    category = guess_category(title + " " + (description or ""))
                    organizer = ev.get("organizer", {}).get("name", "") if ev.get("organizer") else ""

                    event_data = {
                        "title": title,
                        "description": description or f"Événement à {canton}",
                        "date": date_iso,
                        "startTime": start_time,
                        "location": location,
                        "locationText": city or location,
                        "canton": canton,
                        "category": category,
                        "price": "Voir événement",
                        "organizer": organizer,
                        "sourceURL": source_url,
                        "source": "eventbrite",
                        "photoURL": photo_url,
                    }

                    if is_multi_day and end_date_iso:
                        event_data["endDate"] = end_date_iso
                        event_data["isMultiDay"] = True

                    events.append(event_data)

                except Exception as e:
                    print(f"  [Eventbrite] Error parsing event: {e}")
                    continue

            # Pagination
            pagination = data.get("pagination", {})
            if page >= pagination.get("page_count", 1):
                break
            if page >= 5:  # cap at 5 pages per canton
                print(f"  [Eventbrite] Capped at 5 pages")
                break
            page += 1
            time.sleep(0.5)

        except requests.Timeout:
            print(f"  [Eventbrite] Request timed out on page {page}")
            break
        except requests.ConnectionError as e:
            print(f"  [Eventbrite] Connection error: {e}")
            break
        except requests.RequestException as e:
            print(f"  [Eventbrite] Request error: {e}")
            break
        except Exception as e:
            print(f"  [Eventbrite] Unexpected error: {e}")
            traceback.print_exc()
            break

    print(f"  [Eventbrite] Total: {len(events)} events for {canton}")
    return events

# ── Claude API with web search ──────────────────────────────────────────────

def fetch_claude_events(canton):
    """Use Claude API with web search to find local events not on Eventbrite."""
    if anthropic is None:
        print(f"  [Claude] Skipping — anthropic package not installed")
        return []
    if not ANTHROPIC_API_KEY:
        print(f"  [Claude] Skipping — no ANTHROPIC_API_KEY set")
        return []

    print(f"  [Claude] Searching web for events in {canton}...")

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    except Exception as e:
        print(f"  [Claude] Failed to create client: {e}")
        return []

    prompt = (
        f"Recherche sur le web les vrais événements à {canton} en Suisse "
        f"entre juin 2026 et février 2027. "
        f"Cherche sur agenda.ch, sortir.ch, local.ch, eventfrog.ch, "
        f"et les sites officiels du canton. "
        f"Retourne un JSON strict (tableau) avec cette structure pour chaque événement: "
        f'{{"title": "...", "description": "...", "date": "YYYY-MM-DDTHH:MM:SS", '
        f'"endDate": "YYYY-MM-DDTHH:MM:SS ou null si un seul jour", '
        f'"startTime": "HH:mm", "location": "...", "locationText": "...", '
        f'"canton": "{canton}", "category": "...", "price": "... CHF", '
        f'"organizer": "...", "sourceURL": "...", "photoURL": "URL image ou null"}}. '
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

        # Extract text from response blocks
        text = ""
        for block in response.content:
            if hasattr(block, "text"):
                text += block.text

        if not text.strip():
            print(f"  [Claude] Empty response")
            return []

        events = parse_json_response(text)
        print(f"  [Claude] Parsed {len(events)} events from response")

        # Post-process
        for ev in events:
            ev["source"] = "claude_web_search"
            ev["canton"] = canton

            # Fix category
            if "category" not in ev or ev["category"] not in CATEGORIES:
                ev["category"] = guess_category(
                    ev.get("title", "") + " " + ev.get("description", "")
                )

            # Default photo if missing
            if not ev.get("photoURL"):
                ev["photoURL"] = CANTON_DEFAULT_PHOTOS.get(canton, "")

            # Multi-day detection
            if ev.get("endDate"):
                try:
                    sd = datetime.fromisoformat(ev["date"])
                    ed = datetime.fromisoformat(ev["endDate"])
                    if ed.date() > sd.date():
                        ev["isMultiDay"] = True
                    else:
                        del ev["endDate"]
                except Exception:
                    if "endDate" in ev:
                        del ev["endDate"]

        return events

    except Exception as e:
        print(f"  [Claude] Error: {e}")
        traceback.print_exc()
        return []

# ── Helpers ─────────────────────────────────────────────────────────────────

def parse_json_response(text):
    """Extract JSON array from text that might contain markdown fences."""
    text = text.strip()

    # Try direct parse
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass

    # Try markdown code fences
    match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if match:
        try:
            result = json.loads(match.group(1).strip())
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    # Try finding array in text
    match = re.search(r"\[[\s\S]*\]", text)
    if match:
        try:
            result = json.loads(match.group(0))
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

    print(f"  [Parser] Failed to extract JSON array from: {text[:300]}")
    return []


def guess_category(text):
    """Guess event category from title/description text."""
    text_lower = text.lower()
    keywords = {
        "Musique": ["concert", "musique", "jazz", "rock", "orchestre", "dj", "rap", "hip-hop", "chanson"],
        "Sport": ["sport", "course", "marathon", "football", "hockey", "ski", "trail", "cyclisme", "vélo"],
        "Culture": ["culture", "cinéma", "film", "conférence", "lecture", "salon", "livre"],
        "Gastronomie": ["gastronomie", "cuisine", "vin", "dégustation", "food", "brunch", "restaurant"],
        "Nightlife": ["nightlife", "soirée", "club", "dj set", "party", "nuit", "after"],
        "Festival": ["festival", "fête", "carnaval", "festivités"],
        "Marché": ["marché", "brocante", "vide-grenier", "artisanat", "noël"],
        "Théâtre": ["théâtre", "spectacle", "comédie", "danse", "ballet", "opéra"],
        "Exposition": ["exposition", "expo", "musée", "galerie", "art", "peinture", "photo"],
        "Communauté": ["communauté", "bénévol", "association", "rencontre", "atelier", "workshop"],
    }
    for category, kws in keywords.items():
        for kw in kws:
            if kw in text_lower:
                return category
    return "Communauté"


def write_to_firestore(db, events):
    """Write events to Firestore pendingEvents, skipping duplicates in both collections."""
    written = 0
    skipped_dup = 0
    skipped_err = 0

    for event in events:
        title = event.get("title", "").strip()
        canton = event.get("canton", "")
        if not title or not canton:
            skipped_err += 1
            continue

        # Check duplicates in pendingEvents
        try:
            existing = list(
                db.collection("pendingEvents")
                .where("canton", "==", canton)
                .where("title", "==", title)
                .limit(1)
                .get()
            )
            if len(existing) > 0:
                print(f"    Skipping duplicate (pending): {title}")
                skipped_dup += 1
                continue
        except Exception as e:
            print(f"    Warning: pendingEvents check failed: {e}")

        # Check duplicates in approved events
        try:
            existing_approved = list(
                db.collection("events")
                .where("canton", "==", canton)
                .where("title", "==", title)
                .limit(1)
                .get()
            )
            if len(existing_approved) > 0:
                print(f"    Skipping duplicate (approved): {title}")
                skipped_dup += 1
                continue
        except Exception as e:
            print(f"    Warning: events check failed: {e}")

        # Parse date
        date_str = event.get("date", "")
        try:
            dt = datetime.fromisoformat(date_str)
        except Exception:
            print(f"    Skipping bad date: {title} — {date_str}")
            skipped_err += 1
            continue

        # Build Firestore document
        doc_data = {
            "title": title,
            "description": event.get("description", ""),
            "canton": canton,
            "date": dt,
            "price": event.get("price", "Gratuit"),
            "location": event.get("location", canton),
            "locationText": event.get("locationText", event.get("location", canton)),
            "photoURL": event.get("photoURL") or None,
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

        # Multi-day support
        if event.get("isMultiDay") and event.get("endDate"):
            try:
                end_dt = datetime.fromisoformat(event["endDate"])
                doc_data["endDate"] = end_dt
                doc_data["isMultiDay"] = True
            except Exception:
                pass

        try:
            db.collection("pendingEvents").add(doc_data)
            written += 1
            print(f"    Added: {title} [{event.get('source', '?')}]")
        except Exception as e:
            print(f"    Firestore write error for '{title}': {e}")
            skipped_err += 1

    print(f"\n  Results: {written} written, {skipped_dup} duplicates skipped, {skipped_err} errors")
    return written

# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  Real Events Agent — {datetime.now().isoformat()}")
    print("=" * 60)
    print(f"  Eventbrite token: {'set' if EVENTBRITE_TOKEN else 'NOT SET'}")
    print(f"  Anthropic API key: {'set' if ANTHROPIC_API_KEY else 'NOT SET'}")
    print(f"  Firebase SA: {'set' if FIREBASE_SERVICE_ACCOUNT else 'NOT SET'}")
    print(f"  Date range: {DATE_START} to {DATE_END}")
    print()

    db = init_firebase()
    all_events = []
    errors = []

    for canton, coords in CANTONS.items():
        print(f"\n{'─' * 40}")
        print(f"  {canton}")
        print(f"{'─' * 40}")

        # Step 1: Eventbrite
        try:
            eb_events = fetch_eventbrite_events(canton, coords)
            all_events.extend(eb_events)
        except Exception as e:
            msg = f"[Eventbrite/{canton}] {e}"
            print(f"  ERROR: {msg}")
            errors.append(msg)

        # Step 2: Claude web search
        try:
            claude_events = fetch_claude_events(canton)
            all_events.extend(claude_events)
        except Exception as e:
            msg = f"[Claude/{canton}] {e}"
            print(f"  ERROR: {msg}")
            errors.append(msg)

        # Rate limit between cantons
        time.sleep(2)

    # Step 3: Write to Firestore
    print(f"\n{'=' * 60}")
    print(f"  Writing {len(all_events)} events to Firestore")
    print(f"{'=' * 60}")

    try:
        total = write_to_firestore(db, all_events)
    except Exception as e:
        print(f"  FATAL Firestore error: {e}")
        traceback.print_exc()
        total = 0

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  DONE — {total} new events added to pendingEvents")
    if errors:
        print(f"  {len(errors)} errors occurred:")
        for err in errors:
            print(f"    - {err}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
