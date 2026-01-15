import psycopg2
import json
import math
from datetime import datetime
import re
import os
from collections import defaultdict
import dotenv

dotenv.load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# --- Shared Logic ---

def normalize_text(text):
    if not text: return ""
    text = text.lower()
    text = re.sub(r'<[^>]+>', '', text) 
    text = re.sub(r'\([^\)]+\)', '', text) 
    text = re.sub(r'\b(ltd|limited|arch|architects|planning|assoc|associates|consultants|unknown)\b', '', text)
    return " ".join(text.split())

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

# --- Analysis ---

def analyze_churn_agents():
    if not DATABASE_URL:
        print("DATABASE_URL not set")
        return
        
    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    c.execute("SELECT id, decision, registration_date, raw_json FROM applications")
    rows = c.fetchall()
    conn.close()
    
    apps = []
    for r in rows:
        try:
            js = json.loads(r[3])
            js['_id'] = r[0]
            js['_decision'] = r[1] or ''
            
            if r[2]:
                try: js['_dt'] = datetime.fromisoformat(r[2].replace('Z', ''))
                except: js['_dt'] = datetime.min
            else: js['_dt'] = datetime.min
            apps.append(js)
        except: pass
    
    apps.sort(key=lambda x: x['_dt'])
    invalids = [a for a in apps if 'INVALID' in a['_decision'].upper()]
    
    # 1. Map Apps by Applicant for fast lookup
    apps_by_applicant = defaultdict(list)
    for a in apps:
        nm = get_fullname(a)
        if nm: apps_by_applicant[nm].append(a)
        
    # 2. Track Stats per Agent
    # {AgentName: {'invalid_count': 0, 'fired_count': 0, 'retained_count': 0}}
    agent_stats = defaultdict(lambda: {'invalid_count': 0, 'fired_count': 0, 'retained_count': 0})
    
    for inv in invalids:
        inv_id = inv['_id']
        inv_name = get_fullname(inv)
        inv_dt = inv['_dt']
        agent_inv = get_agent(inv)
        
        if not agent_inv or len(agent_inv) < 3: continue
        
        agent_stats[agent_inv]['invalid_count'] += 1
        
        # Find Follow-up
        match = None
        candidates = apps_by_applicant.get(inv_name, [])
        for cand in candidates:
            if cand['_id'] == inv_id: continue
            if cand['_dt'] <= inv_dt: continue
            if location_match(inv, cand):
                match = cand
                break
        
        if match:
            agent_new = get_agent(match)
            
            # Churn Logic
            is_churn = False
            if agent_inv and agent_new:
                if agent_inv == agent_new: is_churn = False
                elif agent_inv in agent_new or agent_new in agent_inv: is_churn = False
                else: is_churn = True
            
            if is_churn:
                agent_stats[agent_inv]['fired_count'] += 1
            else:
                agent_stats[agent_inv]['retained_count'] += 1
    
    # 3. Output
    results = []
    for agent, stats in agent_stats.items():
        if stats['fired_count'] > 0:
            # Only consider "known outcomes" (fired + retained) for rate
            known_outcomes = stats['fired_count'] + stats['retained_count']
            loss_rate = (stats['fired_count'] / known_outcomes) * 100 if known_outcomes > 0 else 0
            
            results.append({
                'name': agent,
                'invalid': stats['invalid_count'],
                'fired': stats['fired_count'],
                'retained': stats['retained_count'],
                'loss_rate': loss_rate
            })
            
    # Sort by "Times Fired"
    by_fired = sorted(results, key=lambda x: x['fired'], reverse=True)
    
    print("--- Agents who got DROPPED after Invalidation (Top 20) ---")
    print(f"{'Agent Name':<30} | {'Invlds':<6} | {'Fired':<5} | {'Retained':<8} | {'Loss Rate %':<10}")
    print("-" * 75)
    for r in by_fired[:20]:
        print(f"{r['name'][:30]:<30} | {r['invalid']:<6} | {r['fired']:<5} | {r['retained']:<8} | {r['loss_rate']:.1f}%")

if __name__ == "__main__":
    analyze_churn_agents()
