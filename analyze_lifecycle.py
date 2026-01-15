import psycopg2
import json
import math
from datetime import datetime
import re
import os
import dotenv
from shared_utils import normalize_text, get_fullname, get_agent, location_match

dotenv.load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

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
    
    stats = {
        'total_invalids': total_invalids,
        'follow_up_rate': 0,
        'abandon_rate': 0,
        'avg_days_to_reapply': 0,
        'architect_churn_rate': 0
    }

    if total_invalids > 0:
        follow_rate = (followed_up_count / total_invalids) * 100
        abandon_rate = (abandoned_count / total_invalids) * 100
        avg_days = total_days / followed_up_count if followed_up_count else 0
        churn_rate = (architect_churn_count / followed_up_count) * 100 if followed_up_count else 0
        
        print(f"Follow-up Rate: {follow_rate:.1f}% ({followed_up_count})")
        print(f"Abandonment Rate: {abandon_rate:.1f}% ({abandoned_count})")
        print(f"Avg Time to Re-apply: {avg_days:.1f} days")
        print(f"Architect Churn Rate: {churn_rate:.1f}% ({architect_churn_count} changes)")
        
        stats.update({
            'follow_up_rate': follow_rate,
            'abandon_rate': abandon_rate,
            'avg_days_to_reapply': avg_days,
            'architect_churn_rate': churn_rate
        })
        
        # print("\nSample Churns:")
        # for i in range(min(5, len(matched_pairs))):
        #    print(f"  App {matched_pairs[i][0]} -> {matched_pairs[i][1]}: '{matched_pairs[i][2]}' -> '{matched_pairs[i][3]}'")
        
    else:
        print("No invalid applications found to analyze.")
        
    return stats

if __name__ == "__main__":
    analyze_lifecycle()
