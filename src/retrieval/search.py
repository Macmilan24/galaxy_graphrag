import google.generativeai as genai
from src.graph_db.neo4j_manager import Neo4jManager
from src.utils.embeddings import EmbeddingService
from config import settings
from src.utils.logger import get_logger

logger = get_logger("retrieval_search")

class GlobalSearch:
    """
    Community-centric search.
    Uses the LLM to select the best matching community for a high-level query.
    """
    def __init__(self):
        self.neo4j = Neo4jManager()
        if not settings.GEMINI_API_KEY:
             raise ValueError("GEMINI_API_KEY is not set.")
        genai.configure(api_key=settings.GEMINI_API_KEY)
        self.model = genai.GenerativeModel('gemini-2.5-pro')

    def search(self, query):
        logger.info(f"Performing Global Search for: '{query}'")
        
        # Fetch all community summaries
        cypher = "MATCH (c:Community) RETURN c.id AS id, c.name AS name, c.summary AS summary"
        communities = self.neo4j.execute_query(cypher)
        
        if not communities:
            return "No communities found in the graph."

        # Ask LLM to pick the best one
        community_text = "\n".join([f"ID {c['id']}: {c['name']} - {c['summary']}" for c in communities])
        
        prompt = f"""
        You are an intelligent assistant for a Galaxy bioinformatics graph.
        
        User Query: "{query}"
        
        Available Communities:
        {community_text}
        
        1. Identify the single most relevant community for this query.
        2. Explain why you chose it.
        3. Return the Community ID.
        
        Format:
        Community_ID: <id>
        Reasoning: <text>
        """
        
        response = self.model.generate_content(prompt)
        return response.text

class LocalSearch:
    """
    Entity-centric search.
    Finds specific tools via Vector Search and explores their immediate neighborhood.
    """
    def __init__(self):
        self.neo4j = Neo4jManager()
        self.embedder = EmbeddingService()

    def search(self, query, top_k=3):
        logger.info(f"Performing Local Search for: '{query}'")
        
        # Generate query embedding
        query_embedding = self.embedder.generate_embedding(query)
        if not query_embedding:
            return []

        # Vector Search for Tools
        cypher = """
        CALL db.index.vector.queryNodes('tool_embeddings', $k, $embedding)
        YIELD node, score
        RETURN node.id AS id, node.name AS name, node.description AS description, score
        """
        
        results = self.neo4j.execute_query(cypher, {"k": top_k, "embedding": query_embedding})
        
        enhanced_results = []
        for tool in results:
            # Expand Neighborhood (1-hop)
            context_query = """
            MATCH (t:Tool {id: $id})
            OPTIONAL MATCH (t)-[:ACCEPTS_INPUT]->(i:FileFormat)
            OPTIONAL MATCH (t)-[:PRODUCES_OUTPUT]->(o:FileFormat)
            OPTIONAL MATCH (w:Workflow)-[:HAS_STEP]->(:WorkflowStep)-[:USES_TOOL]->(t)
            RETURN collect(DISTINCT i.name) AS inputs, 
                   collect(DISTINCT o.name) AS outputs, 
                   collect(DISTINCT w.name)[0..3] AS workflows
            """
            context = self.neo4j.execute_query(context_query, {"id": tool['id']})[0]
            
            enhanced_results.append({
                "tool": tool,
                "context": context
            })
            
        return enhanced_results

class HybridSearch:
    """
    Combines Vector Search with Graph Filters.
    """
    def __init__(self):
        self.neo4j = Neo4jManager()
        self.embedder = EmbeddingService()

    def search(self, query, input_format=None, top_k=5):
        logger.info(f"Performing Hybrid Search for: '{query}' (Filter: {input_format})")
        
        query_embedding = self.embedder.generate_embedding(query)
        
        # Base Vector Search
        cypher = """
        CALL db.index.vector.queryNodes('tool_embeddings', $k, $embedding)
        YIELD node, score
        """
        
        # Apply Graph Filter if provided
        if input_format:
            cypher += f"""
            MATCH (node)-[:ACCEPTS_INPUT]->(f:FileFormat)
            WHERE f.name CONTAINS '{input_format}'
            """
            
        cypher += " RETURN node.name AS name, node.description AS description, score"
        
        return self.neo4j.execute_query(cypher, {"k": top_k, "embedding": query_embedding})
