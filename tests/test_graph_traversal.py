import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.graph_db.neo4j_manager import Neo4jManager
from src.utils.embeddings import EmbeddingService
from src.utils.logger import get_logger

logger = get_logger("test_traversal")

class GraphTester:
    def __init__(self):
        self.neo4j = Neo4jManager()
        self.embedder = EmbeddingService()

    def test_vector_search(self, query, top_k=3):
        """Find tools semantically similar to the query."""
        logger.info(f"--- Testing Vector Search: '{query}' ---")
        embedding = self.embedder.generate_embedding(query)
        
        # Cypher for vector search
        cypher = """
        CALL db.index.vector.queryNodes('tool_embeddings', $k, $embedding)
        YIELD node, score
        RETURN node.name AS name, node.description AS description, score
        """
        
        results = self.neo4j.execute_query(cypher, {"k": top_k, "embedding": embedding})
        for r in results:
            print(f"[Score: {r['score']:.4f}] {r['name']}")
        return results

    def test_graph_traversal(self, tool_name):
        """Find workflows that use a specific tool."""
        logger.info(f"--- Testing Graph Traversal for Tool: '{tool_name}' ---")
        
        cypher = """
        MATCH (t:Tool {name: $tool_name})<-[:USES_TOOL]-(ws:WorkflowStep)<-[:HAS_STEP]-(w:Workflow)
        RETURN DISTINCT w.name AS workflow, w.num_steps AS steps
        LIMIT 5
        """
        
        results = self.neo4j.execute_query(cypher, {"tool_name": tool_name})
        if not results:
            print("No workflows found using this tool.")
        for r in results:
            print(f"Workflow: {r['workflow']} ({r['steps']} steps)")

    def test_hybrid_search(self, query, input_format):
        """Find tools similar to query AND accepting a specific format."""
        logger.info(f"--- Testing Hybrid Search: '{query}' + Input: {input_format} ---")
        embedding = self.embedder.generate_embedding(query)
        
        cypher = """
        CALL db.index.vector.queryNodes('tool_embeddings', 10, $embedding)
        YIELD node, score
        MATCH (node)-[:ACCEPTS_INPUT]->(f:FileFormat {name: $format})
        RETURN node.name AS name, score
        ORDER BY score DESC
        LIMIT 3
        """
        
        results = self.neo4j.execute_query(cypher, {"embedding": embedding, "format": input_format})
        for r in results:
            print(f"[Score: {r['score']:.4f}] {r['name']}")

    def test_workflow_search(self, query, top_k=3):
        """Find workflows semantically similar to the query."""
        logger.info(f"--- Testing Workflow Vector Search: '{query}' ---")
        embedding = self.embedder.generate_embedding(query)
        
        cypher = """
        CALL db.index.vector.queryNodes('workflow_embeddings', $k, $embedding)
        YIELD node, score
        RETURN node.name AS name, node.num_steps AS steps, score
        """
        
        results = self.neo4j.execute_query(cypher, {"k": top_k, "embedding": embedding})
        for r in results:
            print(f"[Score: {r['score']:.4f}] {r['name']} ({r['steps']} steps)")
        return results

    def test_compatible_tools(self, tool_name):
        """Find tools that can accept the output of the given tool."""
        logger.info(f"--- Testing Compatible Tools (Next Steps) for: '{tool_name}' ---")
        
        cypher = """
        MATCH (t1:Tool {name: $tool_name})-[:PRODUCES_OUTPUT]->(f:FileFormat)<-[:ACCEPTS_INPUT]-(t2:Tool)
        RETURN DISTINCT t2.name AS next_tool, f.name AS via_format
        LIMIT 5
        """
        
        results = self.neo4j.execute_query(cypher, {"tool_name": tool_name})
        if not results:
            print("No compatible next tools found.")
        for r in results:
            print(f"Next Tool: {r['next_tool']} (via {r['via_format']})")

    def test_category_search(self, category_name):
        """Find all tools in a category."""
        logger.info(f"--- Testing Category Search: '{category_name}' ---")
        
        cypher = """
        MATCH (t:Tool)-[:BELONGS_TO]->(c:Category)
        WHERE c.name CONTAINS $cat_name
        RETURN t.name AS tool, c.name AS category
        LIMIT 5
        """
        
        results = self.neo4j.execute_query(cypher, {"cat_name": category_name})
        for r in results:
            print(f"Tool: {r['tool']} [{r['category']}]")

    def close(self):
        self.neo4j.close()

if __name__ == "__main__":
    tester = GraphTester()
    try:
        # 1. Vector Search
        tools = tester.test_vector_search("quality control for raw reads")
        
        # 1.5 Workflow Vector Search
        tester.test_workflow_search("RNA-seq analysis")
        
        if tools:
            first_tool = tools[0]['name']
            
            # 2. Graph Traversal: Workflows using this tool
            tester.test_graph_traversal(first_tool)
            
            # 3. Graph Traversal: Compatible next tools
            tester.test_compatible_tools(first_tool)
            
        # 4. Hybrid Search
        tester.test_hybrid_search("align reads", "fastq")

        # 5. Category Search
        tester.test_category_search("Fastq")
        
    finally:
        tester.close()
