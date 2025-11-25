import networkx as nx
import numpy as np
from src.graph_db.neo4j_manager import Neo4jManager
from src.utils.logger import get_logger
from sklearn.metrics.pairwise import cosine_similarity

logger = get_logger("graph_projector")

class GraphProjector:
    def __init__(self):
        self.neo4j = Neo4jManager()

    def fetch_tool_embeddings(self):
        """Fetches tool IDs and their embeddings."""
        logger.info("Fetching tool embeddings...")
        query = "MATCH (t:Tool) WHERE t.embedding IS NOT NULL RETURN t.id AS id, t.embedding AS embedding"
        results = self.neo4j.execute_query(query)
        
        tool_embeddings = {}
        for r in results:
            tool_embeddings[r['id']] = np.array(r['embedding'])
        return tool_embeddings

    def fetch_workflow_cooccurrences(self):
        """Fetches pairs of tools that appear in the same workflow."""
        logger.info("Fetching workflow co-occurrences...")

        query = """
        MATCH (w:Workflow)-[:HAS_STEP]->(s1:WorkflowStep)-[:USES_TOOL]->(t1:Tool)
        MATCH (w)-[:HAS_STEP]->(s2:WorkflowStep)-[:USES_TOOL]->(t2:Tool)
        WHERE t1.id < t2.id 
        RETURN t1.id AS source, t2.id AS target, count(w) AS weight
        """
        results = self.neo4j.execute_query(query)
        return results

    def fetch_io_connections(self):
        """Fetches pairs of tools connected by FileFormat."""
        logger.info("Fetching Input/Output connections...")
        query = """
        MATCH (t1:Tool)-[:PRODUCES_OUTPUT]->(f:FileFormat)<-[:ACCEPTS_INPUT]-(t2:Tool)
        WHERE t1.id <> t2.id
        RETURN t1.id AS source, t2.id AS target, count(f) AS weight
        """
        results = self.neo4j.execute_query(query)
        return results

    def build_weighted_graph(self):
        """Builds a NetworkX graph with weighted edges."""
        G = nx.Graph()
        
        # Nodes & Semantic Similarity
        embeddings = self.fetch_tool_embeddings()
        
        # Filter embeddings to ensure consistent dimension (384)
        valid_tool_ids = []
        valid_embeddings = []
        expected_dim = 384
        
        for tid, emb in embeddings.items():
            if len(emb) == expected_dim:
                valid_tool_ids.append(tid)
                valid_embeddings.append(emb)
            else:
                logger.warning(f"Skipping tool {tid}: Embedding dimension {len(emb)} != {expected_dim}")
        
        tool_ids = valid_tool_ids
        if not tool_ids:
            logger.error("No valid embeddings found with dimension 384.")
            return G

        matrix = np.array(valid_embeddings)
        
        # Add nodes
        G.add_nodes_from(tool_ids)
        
        # Calculate Cosine Similarity 
        logger.info("Calculating semantic similarity...")
        if len(tool_ids) > 0:
            sim_matrix = cosine_similarity(matrix)
            threshold = 0.7 
            
            rows, cols = np.where(sim_matrix > threshold)
            for r, c in zip(rows, cols):
                if r < c: # Upper triangle
                    weight = sim_matrix[r, c]
                    G.add_edge(tool_ids[r], tool_ids[c], weight=weight, type='semantic')

        # Workflow Co-occurrence
        cooccurrences = self.fetch_workflow_cooccurrences()
        for row in cooccurrences:
            u, v, w = row['source'], row['target'], row['weight']
            if G.has_edge(u, v):
                G[u][v]['weight'] += w * 1.0 
            else:
                G.add_edge(u, v, weight=w * 1.0, type='workflow')

        # I/O Connections
        io_conns = self.fetch_io_connections()
        for row in io_conns:
            u, v, w = row['source'], row['target'], row['weight']
            if G.has_edge(u, v):
                G[u][v]['weight'] += w * 0.5 
            else:
                G.add_edge(u, v, weight=w * 0.5, type='io')
                
        logger.info(f"Built graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
        return G

    def close(self):
        self.neo4j.close()
