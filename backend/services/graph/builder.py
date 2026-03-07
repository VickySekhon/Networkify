"""
Parses LinkedIn CSV and builds the Neo4j graph.

CSV columns (LinkedIn export):
  First Name, Last Name, Email Address, Company, Position, Connected On, LinkedIn URL
"""
import pandas as pd
import hashlib
import re
from db.neo4j_client import db
from db.redis_client import redis_client
from config import settings

LOGO_DEV_TOKEN = settings.logo_dev_token


def company_to_logo_url(company_name: str) -> str:
    """Convert a company name to a logo.dev image URL.
    Guesses the domain from the company name (e.g. 'Google' -> 'google.com')."""
    if not company_name:
        return ""
        
    cache_key = f"logo:{company_name}"
    cached = redis_client.get(cache_key)
    if cached:
        return cached

    # Clean the name: remove Inc, Ltd, Corp, LLC, etc.
    clean = re.sub(r'\b(inc|ltd|llc|corp|corporation|co|company|group|technologies|tech|software|solutions|labs|limited|plc)\b',
                   '', company_name, flags=re.IGNORECASE)
    clean = re.sub(r'[^a-zA-Z0-9\s]', '', clean).strip()
    # Convert to domain-like slug
    slug = clean.lower().replace(' ', '')
    if not slug:
        return ""
    domain = f"{slug}.com"
    url = f"https://img.logo.dev/{domain}?token={LOGO_DEV_TOKEN}&size=64"
    
    redis_client.setex(cache_key, 604800, url) # Cache for 7 days
    return url

def parse_csv(file_bytes: bytes) -> pd.DataFrame:
    import io
    # LinkedIn CSVs have a 3-line header — skip it
    df = pd.read_csv(io.BytesIO(file_bytes), skiprows=3)
    df.columns = df.columns.str.strip()
    df = df.fillna("")
    
    df["Initials"] = df["First Name"].str[0] + df["Last Name"].str[0]
    
    return df

def make_id(name: str, email: str = "") -> str:
    raw = f"{name}{email}".lower().strip()
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def build_graph(df: pd.DataFrame, user: dict) -> dict:
    """
    Nodes:
      (:Person {id, name, title, email, profile_url, connected_on, is_recruiter, initials})
      (:Company {name})
    Relationships:
      (you)-[:KNOWS]->(connection)
      (connection)-[:WORKS_AT]->(company)
      (you)-[:WORKS_AT]->(your_companies)
    """
    stats = {"persons": 0, "companies": 0, "relationships": 0}

    # Create the user node variables
    user_initials = "".join([part[0].upper() for part in user["name"].split() if part])
    user_id = user.get("id") or make_id(user["name"])

    recruiter_keywords = ["recruiter", "talent acquisition", "hiring", "hr", "people ops", "talent partner"]
    
    batch = []

    for _, row in df.iterrows():
        name = f"{row.get('First Name', '')} {row.get('Last Name', '')}".strip()
        if not name:
            continue

        email = row.get("Email Address", "")
        company = row.get("Company", "").strip()
        title = row.get("Position", "").strip()
        connected_on = row.get("Connected On", "")
        profile_url = row.get("URL", "")
        person_id = make_id(name, email)

        is_recruiter = any(kw in title.lower() for kw in recruiter_keywords)
        initials = row.get("Initials", "")
        logo_url = company_to_logo_url(company) if company else ""

        batch.append({
            "person_id": person_id,
            "name": name,
            "email": email,
            "company": company,
            "title": title,
            "connected_on": connected_on,
            "profile_url": profile_url,
            "is_recruiter": is_recruiter,
            "initials": initials,
            "logo_url": logo_url
        })
        
    if not batch:
        return stats
        
    query = """
        MERGE (u:Person {id: $user_id})
        SET u.name = $user_name, u.title = $user_title, u.is_user = true, u.initials = $user_initials
        
        WITH u
        UNWIND $batch AS row
        
        MERGE (c:Person {id: row.person_id})
        SET c.name = row.name,
            c.title = row.title,
            c.email = row.email,
            c.profile_url = row.profile_url,
            c.connected_on = row.connected_on,
            c.is_recruiter = row.is_recruiter,
            c.degree = 1,
            c.initials = row.initials
            
        MERGE (u)-[:KNOWS]->(c)
        
        FOREACH (_ IN CASE WHEN row.company <> "" THEN [1] ELSE [] END |
            MERGE (comp:Company {name: row.company})
            SET comp.logo = row.logo_url
            MERGE (c)-[:WORKS_AT]->(comp)
        )
    """

    db.run_write(query, 
        user_id=user_id,
        user_name=user["name"],
        user_title=user.get("title", ""),
        user_initials=user_initials,
        batch=batch
    )
    
    stats["persons"] = len(batch)
    stats["relationships"] = len(batch) * 2 # approximation
    stats["companies"] = len(set([b["company"] for b in batch if b["company"]]))

    return stats
