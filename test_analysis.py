from analyze_agents import analyze_agents
from analyze_churn_agents import analyze_churn_agents
from analyze_invalid import analyze_detailed_failures
from analyze_lifecycle import analyze_lifecycle
from analyze_spread import analyze_spread
import json
import os
from datetime import datetime

print("Testing Analysis Pipeline...", flush=True)

try:
    analysis_results = {
        "timestamp": datetime.now().isoformat(),
        "agents": analyze_agents(),
        "churn": analyze_churn_agents(),
        "failures": analyze_detailed_failures(),
        "lifecycle": analyze_lifecycle(),
        "spread": analyze_spread()
    }
    
    out_dir = "out"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
        
    out_file = os.path.join(out_dir, "test_latest.json")
    
    with open(out_file, 'w') as f:
        json.dump(analysis_results, f, indent=2)
        
    print(f"Success! Written to {out_file}", flush=True)
    
except Exception as e:
    print(f"FAILED: {e}", flush=True)
