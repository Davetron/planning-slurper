import psycopg2
import json
import math
from datetime import datetime
import re
import os
from collections import defaultdict
import dotenv
from shared_utils import normalize_text, get_fullname, get_agent, location_match

dotenv.load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

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
            # JSONB fix
            js = r[3]
            if isinstance(js, str):
                js = json.loads(js)
            else:
                js = js.copy()

            js['_id'] = r[0]
            js['_decision'] = r[1] or ''
            
            # Date fix
            if r[2]:
                if isinstance(r[2], str):
                    try: js['_dt'] = datetime.fromisoformat(r[2].replace('Z', ''))
                    except: js['_dt'] = datetime.min
                else:
                    # It's a date or datetime object
                     if hasattr(r[2], 'combine'): 
                        js['_dt'] = datetime.combine(r[2], datetime.min.time())
                     else:
                        js['_dt'] = r[2]
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
        
    return by_fired

if __name__ == "__main__":
    analyze_churn_agents()
