import psycopg2
import json
import math
from datetime import datetime
import re
import os
import dotenv

dotenv.load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def normalize_text(text):
    if not text: return ""
    # Lowercase, remove emails, remove ref codes
    text = text.lower()
    text = re.sub(r'<[^>]+>', '', text) # Remove emails <foo@bar.com>
    text = re.sub(r'\([^\)]+\)', '', text) # Remove parens
    # Remove common suffixes
    text = re.sub(r'\b(ltd|limited|arch|architects|planning|assoc|associates|consultants|unknown)\b', '', text)
    # Remove whitespace
    return " ".join(text.split())

def get_fullname(raw_app):
    """Constructs applicant name from JSON fields."""
    fore = raw_app.get('applicantForename') or ''
    sur = raw_app.get('applicantSurname') or ''
    return normalize_text(f"{fore} {sur}")

def get_agent(raw_app):
    """Constructs agent name."""
    # Prioritize agentContactName as it's often the full name
    name = raw_app.get('agentContactName') or raw_app.get('agentName') or ''
    sur = raw_app.get('agentSurname') or ''
    if not name and sur: name = sur
    return normalize_text(name)

def location_match(app1, app2):
    """
    Checks if locations match.
    """
    # Grid Match (Best)
    try:
        x1, y1 = app1.get('easting'), app1.get('northing')
        x2, y2 = app2.get('easting'), app2.get('northing')
        if x1 and y1 and x2 and y2:
            dist = math.sqrt((x1-x2)**2 + (y1-y2)**2)
            if dist < 50: # 50 meters tolerance
                return True
    except: pass
    
    # String Match (Fallback)
    def clean_loc(loc):
        loc = loc.lower().replace(',', ' ').replace('.', '')
        # Normalize abbreviations
        loc = re.sub(r'\bst\b', 'street', loc)
        loc = re.sub(r'\brd\b', 'road', loc)
        loc = re.sub(r'\bave\b', 'avenue', loc)
        return set(loc.split())
        
    loc1 = clean_loc(app1.get('location', ''))
    loc2 = clean_loc(app2.get('location', ''))
    
    # Filter common words
    common = {'at', 'the', 'of', 'site', 'land', 'co', 'dublin', 'road', 'street', 'avenue', 'house', 'development', 'permission'}
    loc1 = loc1 - common
    loc2 = loc2 - common
    
    if not loc1 or not loc2: return False
    
    # Jaccard index
    overlap = len(loc1.intersection(loc2))
    union = len(loc1.union(loc2))
    
    if union > 0 and (overlap / union) > 0.6: 
        return True
        
    return False

def analyze_lifecycle():
    if not DATABASE_URL:
        print("DATABASE_URL not set")
        return

    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    
    # Get all apps
    c.execute("SELECT id, decision, registration_date, raw_json FROM applications")
    rows = c.fetchall()
    conn.close()
    
    apps = []
    for r in rows:
        try:
            js = json.loads(r[3])
            js['_id'] = r[0]
            js['_decision'] = r[1] or ''
            js['_reg_date'] = r[2]
            
            if r[2]:
                try:
                    js['_dt'] = datetime.fromisoformat(r[2].replace('Z', ''))
                except:
                    js['_dt'] = datetime.min
            else:
                js['_dt'] = datetime.min
                
            apps.append(js)
        except: pass
        
    apps.sort(key=lambda x: x['_dt'])
    
    invalids = [a for a in apps if 'INVALID' in a['_decision'].upper()]
    
    total_invalids = len(invalids)
    followed_up_count = 0
    abandoned_count = 0
    total_days = 0
    architect_churn_count = 0
    
    # Matching Stats
    print(f"Analyzing {total_invalids} Invalid Applications...")
    
    # Speed optimization: Create a lookup by normalized Applicant Name
    # (Since we only care about same-applicant follow-ups)
    apps_by_applicant = {}
    for a in apps:
        nm = get_fullname(a)
        if nm:
            if nm not in apps_by_applicant: apps_by_applicant[nm] = []
            apps_by_applicant[nm].append(a)
            
    matched_pairs = []
    
    for inv in invalids:
        inv_id = inv['_id']
        inv_name = get_fullname(inv)
        inv_dt = inv['_dt']
        
        match = None
        candidates = apps_by_applicant.get(inv_name, [])
        
        for cand in candidates:
            if cand['_id'] == inv_id: continue
            if cand['_dt'] <= inv_dt: continue # Must be strictly later
            
            # Location Match
            if location_match(inv, cand):
               match = cand
               break # Found earliest match
        
        if match:
            followed_up_count += 1
            delta = (match['_dt'] - inv_dt).days
            total_days += delta
            
            agent_inv = get_agent(inv)
            agent_new = get_agent(match)
            
            # Fuzzy Agent Match
            # If Levenshtein distance is small or token overlap is high, consider same.
            # Here we used simple string equality on normalized text.
            # Let's improve: if one contains the other.
            is_churn = False
            if agent_inv and agent_new:
                if agent_inv == agent_new:
                    is_churn = False
                elif agent_inv in agent_new or agent_new in agent_inv:
                    is_churn = False # Likely same (e.g. "Bob Smith" vs "Bob Smith Arch")
                else:
                    is_churn = True # Truly different
                    
            if is_churn:
                architect_churn_count += 1
                matched_pairs.append((inv_id, match['_id'], agent_inv, agent_new))
        else:
            abandoned_count += 1

    print("\n--- Invalid Application Lifecycle Analysis (Refined) ---")
    print(f"Total Invalid Applications: {total_invalids}")
    
    if total_invalids > 0:
        follow_rate = (followed_up_count / total_invalids) * 100
        abandon_rate = (abandoned_count / total_invalids) * 100
        avg_days = total_days / followed_up_count if followed_up_count else 0
        churn_rate = (architect_churn_count / followed_up_count) * 100 if followed_up_count else 0
        
        print(f"Follow-up Rate: {follow_rate:.1f}% ({followed_up_count})")
        print(f"Abandonment Rate: {abandon_rate:.1f}% ({abandoned_count})")
        print(f"Avg Time to Re-apply: {avg_days:.1f} days")
        print(f"Architect Churn Rate: {churn_rate:.1f}% ({architect_churn_count} changes)")
        
        # print("\nSample Churns:")
        # for i in range(min(5, len(matched_pairs))):
        #    print(f"  App {matched_pairs[i][0]} -> {matched_pairs[i][1]}: '{matched_pairs[i][2]}' -> '{matched_pairs[i][3]}'")
        
    else:
        print("No invalid applications found to analyze.")

if __name__ == "__main__":
    analyze_lifecycle()
