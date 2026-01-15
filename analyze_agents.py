import psycopg2
import json
import re
import os
import csv
import sys
from collections import defaultdict, Counter
import dotenv
from shared_utils import normalize_text, extract_email, get_agent

dotenv.load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def analyze_agents():
    if not DATABASE_URL:
        print("DATABASE_URL not set")
        return

    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    c.execute("SELECT decision, raw_json FROM applications")
    rows = c.fetchall()
    conn.close()
    
    agent_stats = defaultdict(lambda: {'total': 0, 'invalid': 0, 'emails': Counter(), 'phones': Counter()})
    
    for r in rows:
        try:
            decision = (r[0] or "").upper()
            js = json.loads(r[1])
            agent = get_agent(js)
            
            if agent == "unknown/none" or len(agent) < 3:
                continue
                
            agent_stats[agent]['total'] += 1
            if 'INVALID' in decision:
                agent_stats[agent]['invalid'] += 1
                
            # Contact Details
            email_raw = js.get('agentEmail')
            if email_raw:
                email = extract_email(email_raw)
                if email: agent_stats[agent]['emails'][email] += 1
                
            phone = js.get('agentTelephoneNumber')
            if phone:
                agent_stats[agent]['phones'][phone.strip()] += 1
                
        except: pass
        
    # Convert to list for sorting
    results = []
    for agent, stats in agent_stats.items():
        if stats['total'] > 0:
            rate = (stats['invalid'] / stats['total']) * 100
            
            # Get most common contact info
            best_email = stats['emails'].most_common(1)[0][0] if stats['emails'] else ""
            best_phone = stats['phones'].most_common(1)[0][0] if stats['phones'] else ""
            
            results.append({
                'name': agent,
                'total': stats['total'],
                'invalid': stats['invalid'],
                'rate': rate,
                'email': best_email,
                'phone': best_phone
            })
            
    # Sort by Most Invalidations
    by_volume = sorted(results, key=lambda x: x['invalid'], reverse=True)
    
    # Output to stdout instead of CSV
    writer = csv.writer(sys.stdout)
    writer.writerow(['Agent Name', 'Invalid Count', 'Total Applications', 'Invalidation Rate %', 'Email', 'Phone'])
    for r in by_volume[:100]:
        writer.writerow([r['name'], r['invalid'], r['total'], f"{r['rate']:.1f}%", r['email'], r['phone']])
            
    # Optional: Keep the summary tables if user still wants them, or just rely on the CSV output as the main "data"
    # The user asked to "output data to standard out rather than writing files".
    # I have replaced the file writing with sys.stdout writing.
    # I will keep the formatted tables as well as they are useful summaries.
    
    print("\n")
    print(f"--- Top 20 Architects by INVALIDATION VOLUME ---")
    print(f"{'Agent Name':<30} | {'Invld':<5} | {'Rate %':<7} | {'Email'}")
    print("-" * 80)
    for r in by_volume[:20]:
        print(f"{r['name'][:30]:<30} | {r['invalid']:<5} | {r['rate']:.1f}%   | {r['email']}")
        
    print("\n")
    
    # Sort by Rate (min 10 submissions to filter noise)
    min_subs = [r for r in results if r['total'] >= 10]
    by_rate = sorted(min_subs, key=lambda x: x['rate'], reverse=True)
    
    print(f"--- Top 20 Architects by FAILURE RATE (Min 10 Apps) ---")
    print(f"{'Agent Name':<30} | {'Invld':<5} | {'Rate %':<7} | {'Email'}")
    print("-" * 80)
    for r in by_rate[:20]:
        print(f"{r['name'][:30]:<30} | {r['invalid']:<5} | {r['rate']:.1f}%   | {r['email']}")
        
    return by_volume

if __name__ == "__main__":
    analyze_agents()
