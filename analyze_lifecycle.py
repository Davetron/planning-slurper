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
    
    # Get all apps including LPA
    c.execute("SELECT id, decision, registration_date, raw_json, lpa FROM applications")
    rows = c.fetchall()
    conn.close()
    
    apps = []
    for r in rows:
        try:
            js = json.loads(r[3])
            js['_id'] = r[0]
            js['_decision'] = r[1] or ''
            js['_reg_date'] = r[2]
            js['_lpa'] = r[4] or 'unknown'
            
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
    
    # Helper function to calculate stats for a given list of apps
    def calculate_stats(app_list, label="Overall"):
        invalids = [a for a in app_list if 'INVALID' in a['_decision'].upper()]
        total_invalids = len(invalids)
        total_apps = len(app_list)
        
        followed_up_count = 0
        abandoned_count = 0
        total_days = 0
        architect_churn_count = 0
        
        # Build lookup for this specific set of apps
        apps_by_applicant = {}
        for a in app_list:
            nm = get_fullname(a)
            if nm:
                if nm not in apps_by_applicant: apps_by_applicant[nm] = []
                apps_by_applicant[nm].append(a)
        
        for inv in invalids:
            inv_id = inv['_id']
            inv_name = get_fullname(inv)
            inv_dt = inv['_dt']
            
            match = None
            candidates = apps_by_applicant.get(inv_name, [])
            
            for cand in candidates:
                if cand['_id'] == inv_id: continue
                if cand['_dt'] <= inv_dt: continue
                
                if location_match(inv, cand):
                   match = cand
                   break
            
            if match:
                followed_up_count += 1
                delta = (match['_dt'] - inv_dt).days
                total_days += delta
                
                agent_inv = get_agent(inv)
                agent_new = get_agent(match)
                
                is_churn = False
                if agent_inv and agent_new:
                    if agent_inv == agent_new: is_churn = False
                    elif agent_inv in agent_new or agent_new in agent_inv: is_churn = False
                    else: is_churn = True
                        
                if is_churn:
                    architect_churn_count += 1
            else:
                abandoned_count += 1

        # Calculate Rates
        invalidation_rate = (total_invalids / total_apps) * 100 if total_apps > 0 else 0
        follow_rate = (followed_up_count / total_invalids) * 100 if total_invalids > 0 else 0
        abandon_rate = (abandoned_count / total_invalids) * 100 if total_invalids > 0 else 0
        avg_days = total_days / followed_up_count if followed_up_count else 0
        churn_rate = (architect_churn_count / followed_up_count) * 100 if followed_up_count else 0
        
        # Print Summary
        print(f"\n--- {label} Analysis ---")
        print(f"Total Applications: {total_apps}")
        print(f"Total Invalids: {total_invalids}")
        print(f"Invalidation Rate: {invalidation_rate:.1f}%")
        print(f"Follow-up Rate: {follow_rate:.1f}% ({followed_up_count})")
        print(f"Abandonment Rate: {abandon_rate:.1f}% ({abandoned_count})")
        print(f"Avg Time to Re-apply: {avg_days:.1f} days")
        print(f"Architect Churn Rate: {churn_rate:.1f}% ({architect_churn_count} changes)")
        
        return {
            'label': label,
            'total_applications': total_apps,
            'total_invalids': total_invalids,
            'overall_invalidation_rate': invalidation_rate,
            'follow_up_rate': follow_rate,
            'abandon_rate': abandon_rate,
            'avg_days_to_reapply': avg_days,
            'architect_churn_rate': churn_rate
        }

    # 1. Overall Stats
    overall_stats = calculate_stats(apps, "Overall")
    
    # 2. Per LPA Stats
    lpa_groups = {}
    for a in apps:
        l = a.get('_lpa', 'unknown')
        if l not in lpa_groups: lpa_groups[l] = []
        lpa_groups[l].append(a)
        
    lpa_stats = {}
    for lpa, lpa_apps in lpa_groups.items():
        lpa_stats[lpa] = calculate_stats(lpa_apps, f"LPA: {lpa}")
        
    return {
        'overall': overall_stats,
        'by_lpa': lpa_stats
    }

if __name__ == "__main__":
    analyze_lifecycle()
