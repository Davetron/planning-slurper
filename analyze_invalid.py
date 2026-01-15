import psycopg2
import textwrap
import re
import os
from collections import Counter
import dotenv

dotenv.load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def clean_note(text):
    """Extracts the specific note from the long description."""
    if not text:
        return None
    
    # Try to find "Note:" or similar variations
    match = re.search(r'Note\s*:?-?\s*(.*)', text, re.IGNORECASE | re.DOTALL)
    if match:
        # Return the clean text after "Note:", removing newlines
        return " ".join(match.group(1).split()).strip()
    return None

def analyze_detailed_failures():
    if not DATABASE_URL:
        print("DATABASE_URL not set")
        return
        
    conn = psycopg2.connect(DATABASE_URL)
    c = conn.cursor()
    
    # 1. Get Top 10 Categories
    query_cats = """
    SELECT 
        c.short_desc, 
        COUNT(*) as freq
    FROM conditions c
    JOIN applications a ON c.app_id = a.id AND c.lpa = a.lpa
    WHERE a.decision LIKE '%INVALID%'
    GROUP BY c.short_desc
    ORDER BY freq DESC
    LIMIT 30
    """
    
    c.execute(query_cats)
    top_categories = c.fetchall()
    
    print("--- Detailed Invalidation Analysis ---\n")
    
    for rank, (category, count) in enumerate(top_categories, 1):
        print(f"{rank}. {category} (Total: {count})")
        
        # 2. Get all descriptions for this category
        c.execute("""
            SELECT c.long_desc 
            FROM conditions c 
            JOIN applications a ON c.app_id = a.id AND c.lpa = a.lpa
            WHERE a.decision LIKE '%INVALID%' AND c.short_desc = %s
        """, (category,))
        
        descriptions = c.fetchall()
        
        # 3. Extract and Count Specific Notes
        notes = []
        for (desc,) in descriptions:
            note = clean_note(desc)
            if note:
                notes.append(note)
            else:
                # If no "Note:" found, use the first sentence or generic marker
                # useful if the whole text is the specific reason
                notes.append("(Generic/No specific note parsed)")

        note_counts = Counter(notes).most_common(5)
        
        for i, (issue, issue_count) in enumerate(note_counts, 1):
            # Format: '   a. Issue text (Freq: N)'
            
            # Wrap issue text nicely
            wrapped = textwrap.fill(issue, width=90)
            indented = textwrap.indent(wrapped, '      ')
            # Add bullet
            print(f"   {chr(96+i)}. ({issue_count} cases)")
            print(indented)
        
        print("")
        
    conn.close()

if __name__ == "__main__":
    analyze_detailed_failures()
