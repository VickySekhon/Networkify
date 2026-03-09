// ── Find all 1st-degree connections at a company ──────────
MATCH (u:Person {id: $user_id})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE toLower(c.name) CONTAINS toLower($company)
RETURN p, c ORDER BY p.is_recruiter DESC;

// ── Find 2nd-degree paths to a company ───────────────────
MATCH (u:Person {id: $user_id})-[:KNOWS]->(bridge:Person)-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
WHERE toLower(c.name) CONTAINS toLower($company) AND NOT (u)-[:KNOWS]->(p)
RETURN u, bridge, p, c LIMIT 50;

// ── Top companies by connection count ─────────────────────
MATCH (:Person {id: $user_id})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
RETURN c.name AS company, count(p) AS connections
ORDER BY connections DESC LIMIT 20;

// ── All recruiters in network ─────────────────────────────
MATCH (:Person {id: $user_id})-[:KNOWS]->(p:Person)
WHERE p.is_recruiter = true RETURN p ORDER BY p.company;

