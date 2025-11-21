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

    def create_constraints(self, constraints):
        """
        Apply a list of Cypher constraints to the database.
        Example constraint: "CREATE CONSTRAINT FOR (t:Tool) REQUIRE t.id IS UNIQUE"
        """
        assert self._driver is not None, "Driver not initialized"
        with self._driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    print(f"Applied constraint: {constraint}")
                except Exception as e:
                    print(f"Note: Constraint might already exist or failed: {e}")

    def execute_batch(self, query, data, batch_size=1000):
        """
        Execute a query in batches.
        The query should expect a parameter named 'batch'.
        Example: "UNWIND $batch AS row CREATE (n:Node {id: row.id})"
        """
        assert self._driver is not None, "Driver not initialized"
        with self._driver.session() as session:
            total = len(data)
            print(f"Starting batch execution for {total} records...")
            for i in range(0, total, batch_size):
                batch = data[i : i + batch_size]
                session.run(query, {"batch": batch})
                print(f"Processed batch {i // batch_size + 1}/{(total + batch_size - 1) // batch_size}")