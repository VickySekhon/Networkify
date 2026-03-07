from neo4j import GraphDatabase
from config import settings

class Neo4jClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password)
        )

    def verify_connectivity(self):
        try:
            self.driver.verify_connectivity()
            return True
        except Exception as e:
            return False

    def close(self):
        self.driver.close()

    def run(self, query: str, **params):
        with self.driver.session() as session:
            return session.run(query, **params).data()

    def run_write(self, query: str, **params):
        with self.driver.session() as session:
            return session.execute_write(lambda tx: tx.run(query, **params).data())

db = Neo4jClient()
