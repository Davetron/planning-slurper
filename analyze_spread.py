import psycopg2
import os
import dotenv

dotenv.load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def analyze_spread():
    if not DATABASE_URL:
        print("DATABASE_URL not set")
        return

    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    
    # Get all invalidation reasons and their counts
    query = """
    SELECT 
        c.short_desc, 
        COUNT(*) as freq
    FROM conditions c
    JOIN applications a ON c.app_id = a.id
    WHERE a.decision LIKE '%INVALID%'
    GROUP BY c.short_desc
    ORDER BY freq DESC
    """
    
    c.execute(query)
    rows = c.fetchall()
    conn.close()
    
    total_issues = sum(row[1] for row in rows)
    
    print(f"--- Invalidation Spread Analysis ---")
    print(f"Total Invalidation Issues (Reasons Cited): {total_issues}\n")
    
    # 1. Top 10 Coverage
    top_10 = rows[:10]
    top_10_count = sum(row[1] for row in top_10)
    top_10_pct = (top_10_count / total_issues) * 100
    
    print(f"Top 10 Reasons cover: {top_10_count} issues")
    print(f"Top 10 Percentage: {top_10_pct:.2f}%\n")
    
    # 2. 95% Spread
    print("--- Reasons covering 95% of Invalidations ---")
    cumulative = 0
    cutoff = total_issues * 0.95
    
    reasons_list = []
    
    for rank, (reason, count) in enumerate(rows, 1):
        cumulative += count
        pct = (count / total_issues) * 100
        cum_pct = (cumulative / total_issues) * 100
        
        print(f"{rank}. {reason}: {count} ({pct:.1f}%) [Cum: {cum_pct:.1f}%]")
        
        reasons_list.append({
            'rank': rank,
            'reason': reason,
            'count': count,
            'pct': pct,
            'cum_pct': cum_pct
        })
        
        if cumulative >= cutoff:
            print(f"\nReached 95% coverage at Rank {rank}")
            # we keep looping to finish the list or just break?
            # User might want full list in JSON, but typical spreadsheet logic says just the top ones.
            # Let's include ALL in the returned list, but break the print loop as before.
            # Actually, let's keep it consistent: the print loop breaks, so the list will only be 95% coverage.
            # If we want full list, we should iterate separately. 
            # Given the previous code, let's just return what was printed.
            break
            
    return {
        'total_issues': total_issues,
        'top_10_coverage': top_10_count,
        'top_10_pct': top_10_pct,
        'reasons_95_pct': reasons_list
    }

if __name__ == "__main__":
    analyze_spread()
