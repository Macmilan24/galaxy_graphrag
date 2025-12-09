import networkx as nx
import numpy as np
from src.graph_db.neo4j_manager import Neo4jManager
from src.utils.logger import get_logger
from sklearn.metrics.pairwise import cosine_similarity

logger = get_logger("universal_projector")

class UniversalGraphProjector:
    """
    Level 1: Universal Semantic Layer.
    Projects ALL nodes (Tools + Workflows) into a single semantic graph.
    Edges are purely based on Cosine Similarity of embeddings.
    """
    def __init__(self):
        self.neo4j = Neo4jManager()

    def fetch_all_embeddings(self):
        """
        Fetches embeddings for both Tools and Workflows.
        Returns:
            dict: {node_key: embedding_array}
            dict: {node_key: node_type} ('Tool' or 'Workflow')
        
        Node Keys are prefixed: "Tool:<id>" or "Workflow:<id>"
        """
        logger.info("Fetching Universal Embeddings (Tools + Workflows)...")
        
        embeddings = {}
        node_types = {}
        
        # Fetch Tools
        query_tools = "MATCH (t:Tool) WHERE t.embedding IS NOT NULL RETURN t.id AS id, t.embedding AS embedding"
        results_tools = self.neo4j.execute_query(query_tools)
        for r in results_tools:
            # Use prefix to distinguish types and ensure uniqueness
            key = f"Tool:{r['id']}"
            embeddings[key] = np.array(r['embedding'])
            node_types[key] = "Tool"
            
        # Fetch Workflows
        query_workflows = "MATCH (w:Workflow) WHERE w.embedding IS NOT NULL RETURN w.id AS id, w.embedding AS embedding"
        results_workflows = self.neo4j.execute_query(query_workflows)
        for r in results_workflows:
            key = f"Workflow:{r['id']}"
            embeddings[key] = np.array(r['embedding'])
            node_types[key] = "Workflow"
            
        logger.info(f"Fetched {len(embeddings)} entities ({len(results_tools)} Tools, {len(results_workflows)} Workflows).")
        return embeddings, node_types

    def build_universal_graph(self, similarity_threshold=0.7):
        """
        Builds the universal semantic graph.
        """
        G = nx.Graph()
        
        embeddings, node_types = self.fetch_all_embeddings()
        
        # Filter for dimension consistency (384)
        valid_keys = []
        valid_vectors = []
        expected_dim = 384
        
        for key, emb in embeddings.items():
            if len(emb) == expected_dim:
                valid_keys.append(key)
                valid_vectors.append(emb)
            else:
                logger.warning(f"Skipping {key}: Dimension {len(emb)} != {expected_dim}")
        
        if not valid_keys:
            logger.error("No valid embeddings found.")
            return G, node_types

        # Add nodes with type info
        for key in valid_keys:
            G.add_node(key, type=node_types[key])
            
        # Calculate Similarity
        logger.info("Calculating Universal Cosine Similarity...")
        matrix = np.array(valid_vectors)
        sim_matrix = cosine_similarity(matrix)
        
        # Find pairs with high similarity
        rows, cols = np.where(sim_matrix > similarity_threshold)
        
        edge_count = 0
        for r, c in zip(rows, cols):
            if r < c: # Upper triangle
                weight = sim_matrix[r, c]
                G.add_edge(valid_keys[r], valid_keys[c], weight=weight)
                edge_count += 1
                
        logger.info(f"Built Universal Graph: {G.number_of_nodes()} nodes, {edge_count} edges.")
        return G, node_types

    def close(self):
        self.neo4j.close()
