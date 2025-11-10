from neo4j import GraphDatabase
from config import settings

class Neo4jManager:
    
    def __init__(self):
        self.uri = settings.NEO4J_URI
        self.user = settings.NEO4J_USER
        self.password = settings.NEO4J_PASSWORD
        self._driver = None
        try:
            self.connect()
            print("Successfully Connected to Neo4j.")
        except Exception as e:
            print(f"Error: Could not connect to Neo4j. Error: {e}")
            raise
    
    def connect(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
    
    def close(self):
        if self._driver is not None:
            self._driver.close()
            self._driver = None
            print("Neo4j connection closed.")
    
    def execute_query(self, query, parameters=None):
        assert self._driver is not None, "Driver not initialized"
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return [record for record in result]