import re
import math

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

def get_agent(raw_app):
    name = raw_app.get('agentContactName') or raw_app.get('agentName') or ''
    sur = raw_app.get('agentSurname') or ''
    if not name and sur: name = sur
    return normalize_text(name)

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
