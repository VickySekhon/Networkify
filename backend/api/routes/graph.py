from fastapi import APIRouter, Query, Depends
from db.neo4j_client import db
from api.routes.auth import get_current_user

router = APIRouter()


@router.get("/connections")
def get_connections(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    search: str | None = Query(None),
    company: str | None = Query(None),
):
    """Returns paginated 1st-degree connections for the authenticated user."""
    user_id = current_user["id"]
    skip = (page - 1) * page_size

    match_clause = "MATCH (u:Person {id: $user_id})-[:KNOWS]->(p:Person)"
    where_clauses = []
    params = {"user_id": user_id, "skip": skip, "limit": page_size}

    if search:
        where_clauses.append(
            "(toLower(p.name) CONTAINS toLower($search) OR toLower(p.title) CONTAINS toLower($search))"
        )
        params["search"] = search

    if company:
        where_clauses.append("EXISTS { (p)-[:WORKS_AT]->(:Company {name: $company}) }")
        params["company"] = company

    where_str = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""


    # 1. Get total count
    count_query = f"{match_clause}{where_str} RETURN count(p) as total"
    count_result = db.run(count_query, **params)
    total_count = count_result[0]["total"] if count_result else 0

    # 2. Get paginated results

    results_query = f"""
        {match_clause}{where_str}
        OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
        RETURN p, c
        ORDER BY p.name
        SKIP $skip
        LIMIT $limit
    """
    results = db.run(results_query, **params)

    connections = []
    for r in results:
        person = dict(r["p"])
        company_node = dict(r["c"]) if r.get("c") else {}
        person["company"] = company_node.get("name", "")
        person["degree"] = "1st"
        connections.append(person)

    return {
        "connections": connections,
        "total_count": total_count,
        "page": page,
        "page_size": page_size,
    }


@router.get("/companies")
def get_user_companies(current_user: dict = Depends(get_current_user)):
    """Returns all unique companies for the authenticated user's 1st-degree connections."""
    user_id = current_user["id"]
    results = db.run("""

        MATCH (u:Person {id: $user_id})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
        RETURN DISTINCT c.name as name
        ORDER BY name
    """,
        user_id=user_id,
    )
    return [r["name"] for r in results]


@router.get("/stats")

def get_stats(current_user: dict = Depends(get_current_user)):
    """Returns summary stats for the authenticated user's dashboard."""
    user_id = current_user["id"]
    
    person_count = db.run("""

        MATCH (u:Person {id: $user_id})-[:KNOWS]->(p:Person)
        RETURN count(p) as count
    """,
        user_id=user_id,
    )

    company_count = db.run(
        """
        MATCH (u:Person {id: $user_id})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
        RETURN count(DISTINCT c) as count
    """,
        user_id=user_id,
    )

    recruiter_count = db.run(
        """
        MATCH (u:Person {id: $user_id})-[:KNOWS]->(p:Person)
        WHERE p.is_recruiter = true
        RETURN count(p) as count
    """,
        user_id=user_id,
    )

    top_companies = db.run(
        """
        MATCH (u:Person {id: $user_id})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
        RETURN c.name as company, count(p) as connections
        ORDER BY connections DESC
        LIMIT 5
    """,
        user_id=user_id,
    )

    return {
        "connections": person_count[0]["count"] if person_count else 0,
        "companies": company_count[0]["count"] if company_count else 0,
        "recruiters": recruiter_count[0]["count"] if recruiter_count else 0,
        "top_companies": top_companies,
    }


@router.get("/overview")
def get_graph_overview(current_user: dict = Depends(get_current_user)):
    """
    Returns the full graph for the authenticated user's network visualizer.
    
    We:
      - Find all root/source persons where owner_user_id == user_id
        (including the primary user).
      - Expand up to 2 KNOWS hops out from each root to collect people.
      - Attach their companies via WORKS_AT.
      - Add KNOWS edges between any collected people.
    """
    user_id = current_user["id"]

    # 1. Fetch all root/source persons for this owner (primary + imported networks)
    root_rows = db.run(
        """
        MATCH (root:Person)
        WHERE (root.is_user = true OR root.is_source = true)
          AND coalesce(root.owner_user_id, root.id) = $owner_id
        RETURN root
    """,
        owner_id=user_id,
    )

    if not root_rows:
        return {"nodes": [], "links": []}

    # Determine which node is "you" in the visualization
    user_node_row = next(
        (r for r in root_rows if dict(r["root"]).get("id") == user_id), None
    )
    if user_node_row is None:
        user_node_row = root_rows[0]

    user_node = dict(user_node_row["root"])
    user_name = user_node.get("name", "You")


    # 2. Collect all people within up to 2 KNOWS hops of ANY root
    people_rows = db.run(
        """
        MATCH (root:Person)
        WHERE (root.is_user = true OR root.is_source = true)
          AND coalesce(root.owner_user_id, root.id) = $owner_id
        WITH collect(root) AS roots
        MATCH (r:Person)-[:KNOWS*0..2]-(p:Person)
        WHERE r IN roots
        RETURN DISTINCT p
        LIMIT 1000
    """,
        owner_id=user_id,
    )


    # 3. Attach companies for all collected people
    person_ids: list[str] = [dict(r["p"]).get("id") for r in people_rows if dict(r["p"]).get("id")]
    person_ids_set = set(person_ids)

    company_rows = []
    if person_ids:
        company_rows = db.run(
            """
            MATCH (p:Person)-[:WORKS_AT]->(c:Company)
            WHERE p.id IN $ids
            RETURN p.id AS pid, c
        """,
            ids=person_ids,
        )

    # Build maps for quick lookup
    companies_by_person: dict[str, list[dict]] = {}
    for row in company_rows:
        pid = row["pid"]
        c_dict = dict(row["c"])
        companies_by_person.setdefault(pid, []).append(c_dict)

    nodes: list[dict] = []
    links: list[dict] = []
    seen_nodes: set[str] = set()
    seen_companies: set[str] = set()


    # Central user node
    nodes.append(
        {
            "id": user_node.get("id", user_id),
            "name": user_name,
            "type": "user",
            "title": user_node.get("title", ""),
            "initials": user_node.get("initials", ""),
            "is_source": user_node.get("is_source", True),
            "network_name": user_node.get("network_name", "Primary Network"),
        }
    )
    seen_nodes.add(user_node.get("id", user_id))


    # 4. Add all person + company nodes and WORKS_AT links
    for r in people_rows:
        person = dict(r["p"])
        pid = person.get("id", "")
        if not pid:
            continue

        # Skip re-adding the central user as a person node
        if pid != user_node.get("id") and pid not in seen_nodes:
            nodes.append(
                {
                    "id": pid,
                    "name": person.get("name", ""),
                    "type": "person",
                    "title": person.get("title", ""),
                    "is_recruiter": person.get("is_recruiter", False),
                    "initials": person.get("initials", ""),
                    "profile_url": person.get("profile_url", ""),
                    "connected_on": person.get("connected_on", ""),
                    "is_source": person.get("is_source", False),
                    "network_name": person.get("network_name", ""),
                }
            )
            seen_nodes.add(pid)

        # Add companies for this person from the lookup map
        for company in companies_by_person.get(pid, []):
            cname = company.get("name", "")
            if not cname:
                continue
            cid = f"company_{cname}"
            if cid not in seen_companies:
                nodes.append(
                    {
                        "id": cid,
                        "name": cname,
                        "type": "company",
                        "logo": company.get("logo", ""),
                        "url": company.get("url", ""),
                    }
                )
                seen_companies.add(cid)

            links.append(
                {
                    "source": pid,
                    "target": cid,
                    "label": "WORKS_AT",
                }
            )

    # 5. Add KNOWS edges between all collected people (including all networks)
    if person_ids_set:
        edge_rows = db.run(
            """
            MATCH (a:Person)-[:KNOWS]-(b:Person)
            WHERE a.id IN $ids AND b.id IN $ids
            RETURN DISTINCT a.id AS src, b.id AS dst
        """,
            ids=list(person_ids_set),
        )
        seen_edges: set[tuple[str, str]] = set()
        for er in edge_rows:
            src = er["src"]
            dst = er["dst"]
            if src == dst:
                continue
            key = tuple(sorted((src, dst)))
            if key in seen_edges:
                continue
            seen_edges.add(key)
            links.append(
                {
                    "source": src,
                    "target": dst,
                    "label": "KNOWS",
                }
            )


    return {"nodes": nodes, "links": links}


@router.get("/company/{company_name}")
def get_company_subgraph(
    company_name: str,
    current_user: dict = Depends(get_current_user)
):
    """Returns the subgraph relevant to a specific company search."""

    user_id = current_user["id"]
    result = db.run("""
        MATCH (u:Person {id: $user_id})-[:KNOWS]->(p:Person)-[:WORKS_AT]->(c:Company)
        WHERE toLower(c.name) CONTAINS toLower($company)
        RETURN u, p, c
    """,
        user_id=user_id,
        company=company_name,
    )
    return result


@router.get("/networks")
def get_networks(owner_user_id: str = Query(...)):
    """
    Returns a list of source networks (primary + imported) for the given owner.

    Each network is represented by the root/source Person node.
    """
    rows = db.run(
        """
        MATCH (root:Person)
        WHERE (root.is_source = true OR root.is_user = true)
          AND coalesce(root.owner_user_id, root.id) = $owner_user_id
        OPTIONAL MATCH (root)-[:KNOWS]->(p:Person)
        RETURN root, count(DISTINCT p) AS connections
        ORDER BY root.is_user DESC, root.name ASC
    """,
        owner_user_id=owner_user_id,
    )

    networks: list[dict] = []
    for r in rows:
        root = dict(r["root"])
        networks.append(
            {
                "id": root.get("id"),
                "name": root.get("network_name") or root.get("name"),
                "root_person_name": root.get("name"),
                "root_person_title": root.get("title", ""),
                "is_primary": bool(root.get("is_user", False)),
                "connections": r.get("connections", 0),
            }
        )

    return {"networks": networks}
