import re
import math

# Application types that represent substantive planning applications.
# Excludes compliance submissions, S5 declarations, exemption certificates,
# extension of duration, licences, Part 8, and fire certs.
PLANNING_APPLICATION_TYPES = {
    'Permission',
    'Permission and Retention',
    'Permission (LRD)',
    'Permission (SDZ)',
    'Outline Permission',
    'Retention',
    'Permission for Retention',
    'SDZ Application',
    'Perm on foot of Outline permission',
    'Permission and Outline Permission',
    'Perm.consequent on Grant of Outline Perm',
    'Strategic Infrastructure Application',
}

def is_planning_application(raw_json):
    """Returns True if the application is a substantive planning application
    (not compliance, S5, exemption, licence, etc.)."""
    app_type = raw_json.get('applicationType') or ''
    return app_type in PLANNING_APPLICATION_TYPES


def normalize_text(text):
    if not text: return "Unknown/None"
    text = text.lower()
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\([^\)]+\)', '', text)
    text = re.sub(r'\b(ltd|limited|arch|architects|planning|assoc|associates|consultants|unknown|services|design|engineers)\b', '', text)
    text = re.sub(r'[^\w\s]', '', text) # Remove punctuation
    return " ".join(text.split())

def extract_email(text):
    if not text: return ""
    # Try to find email inside < >
    match = re.search(r'<([^>]+)>', text)
    if match:
        return match.group(1).strip()
    # Otherwise assume the whole thing or look for simple email pattern
    match = re.search(r'[\w\.-]+@[\w\.-]+', text)
    if match:
        return match.group(0)
    return text.strip()

def get_fullname(raw_app):
    fore = raw_app.get('applicantForename') or ''
    sur = raw_app.get('applicantSurname') or ''
    return normalize_text(f"{fore} {sur}")

def get_agent(raw_app, dedup_map=None):
    """Returns the normalised agent name for an application.
    If dedup_map is provided, uses the agent's email to resolve to a
    canonical practice name, collapsing spelling variants."""
    if dedup_map:
        email = (raw_app.get('agentEmail') or '').strip().lower()
        if email and email in dedup_map:
            return dedup_map[email]
    name = raw_app.get('agentContactName') or raw_app.get('agentName') or ''
    sur = raw_app.get('agentSurname') or ''
    if not name and sur: name = sur
    return normalize_text(name)


def build_agent_dedup_map(apps):
    """Builds an email -> canonical agent name mapping from a list of raw_json dicts.
    For each email address, the canonical name is the most frequently used
    agentSurname (after normalisation), breaking ties alphabetically."""
    from collections import Counter

    email_names = {}  # email -> Counter of normalised names
    for app in apps:
        email = (app.get('agentEmail') or '').strip().lower()
        surname = app.get('agentSurname') or ''
        if not email or not surname or len(surname) < 3:
            continue
        normalised = normalize_text(surname)
        if normalised == "unknown/none":
            continue
        if email not in email_names:
            email_names[email] = Counter()
        email_names[email][normalised] += 1

    # Pick the most common name for each email
    dedup_map = {}
    for email, name_counts in email_names.items():
        canonical = name_counts.most_common(1)[0][0]
        dedup_map[email] = canonical

    return dedup_map

def location_match(app1, app2):
    try:
        x1, y1 = app1.get('easting'), app1.get('northing')
        x2, y2 = app2.get('easting'), app2.get('northing')
        if x1 and y1 and x2 and y2:
            dist = math.sqrt((x1-x2)**2 + (y1-y2)**2)
            if dist < 50: return True
    except: pass
    
    def clean_loc(loc):
        if not loc: return set()
        loc = loc.lower().replace(',', ' ').replace('.', '')
        loc = re.sub(r'\bst\b', 'street', loc)
        loc = re.sub(r'\brd\b', 'road', loc)
        loc = re.sub(r'\bave\b', 'avenue', loc)
        return set(loc.split())
        
    loc1 = clean_loc(app1.get('location', ''))
    loc2 = clean_loc(app2.get('location', ''))
    common = {'at', 'the', 'of', 'site', 'land', 'co', 'dublin', 'road', 'street', 'avenue', 'house', 'development', 'permission'}
    loc1 -= common
    loc2 -= common
    
    if not loc1 or not loc2: return False
    overlap = len(loc1.intersection(loc2))
    union = len(loc1.union(loc2))
    return union > 0 and (overlap / union) > 0.6

def clean_note(text):
    """Extracts the specific note from the long description."""
    if not text:
        return None
    
    # Try to find "Note:" or similar variations with a separator (colon or hyphen)
    # Using \b to ensure we match "Note" as a whole word
    match = re.search(r'\bNote\s*[:\-]+\s*(.*)', text, re.IGNORECASE | re.DOTALL)
    if match:
        # Return the clean text after "Note:", removing newlines
        return " ".join(match.group(1).split()).strip()
    return None
