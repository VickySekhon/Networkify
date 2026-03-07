"""
Parses LinkedIn CSV and builds the Neo4j graph.

CSV columns (LinkedIn export):
  First Name, Last Name, Email Address, Company, Position, Connected On, LinkedIn URL
"""
import pandas as pd
import hashlib
import re
from db.neo4j_client import db
from config import settings

LOGO_DEV_TOKEN = settings.logo_dev_token


def company_to_logo_url(company_name: str) -> str:
    """Convert a company name to a logo.dev image URL.
    Guesses the domain from the company name (e.g. 'Google' -> 'google.com')."""
    if not company_name:
        return ""
    # Clean the name: remove Inc, Ltd, Corp, LLC, etc.
    clean = re.sub(r'\b(inc|ltd|llc|corp|corporation|co|company|group|technologies|tech|software|solutions|labs|limited|plc)\b',
                   '', company_name, flags=re.IGNORECASE)
    clean = re.sub(r'[^a-zA-Z0-9\s]', '', clean).strip()
    # Convert to domain-like slug
    slug = clean.lower().replace(' ', '')
    if not slug:
        return ""
    domain = f"{slug}.com"
    return f"https://img.logo.dev/{domain}?token={LOGO_DEV_TOKEN}&size=64"

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

    # Create the user node
    user_initials = "".join([part[0].upper() for part in user["name"].split() if part])
    db.run_write("""
        MERGE (p:Person {id: $id})
        SET p.name = $name, p.title = $title, p.is_user = true, p.initials = $initials
    """, id=make_id(user["name"]), name=user["name"], title=user.get("title", ""), initials=user_initials)

    recruiter_keywords = ["recruiter", "talent acquisition", "hiring", "hr", "people ops", "talent partner"]

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

        # Upsert Person
        db.run_write("""
            MERGE (p:Person {id: $id})
            SET p.name = $name,
                p.title = $title,
                p.email = $email,
                p.profile_url = $profile_url,
                p.connected_on = $connected_on,
                p.is_recruiter = $is_recruiter,
                p.degree = 1,
                p.initials = $initials
        """, id=person_id, name=name, title=title, email=email,
             profile_url=profile_url, connected_on=connected_on,
             is_recruiter=is_recruiter, initials=initials)
        stats["persons"] += 1

        # Relationship: you → connection
        db.run_write("""
            MATCH (u:Person {id: $user_id}), (c:Person {id: $conn_id})
            MERGE (u)-[:KNOWS]->(c)
        """, user_id=make_id(user["name"]), conn_id=person_id)
        stats["relationships"] += 1

        if company:
            # Upsert Company with logo
            logo_url = company_to_logo_url(company)
            db.run_write("""
                MERGE (co:Company {name: $name})
                SET co.logo = $logo
            """, name=company, logo=logo_url)
            stats["companies"] += 1

            # Relationship: connection → company
            db.run_write("""
                MATCH (p:Person {id: $person_id}), (co:Company {name: $company})
                MERGE (p)-[:WORKS_AT]->(co)
            """, person_id=person_id, company=company)
            stats["relationships"] += 1

    return stats
