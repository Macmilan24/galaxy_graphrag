import leidenalg
import igraph as ig
import networkx as nx
from src.community_detection.graph_projector import GraphProjector
from src.graph_db.neo4j_manager import Neo4jManager
from src.utils.logger import get_logger

logger = get_logger("leiden_detector")

class LeidenDetector:
    # Orchestrates community detection: Project Graph -> Convert to iGraph -> Run Leiden -> Update Neo4j
    def __init__(self):
        self.projector = GraphProjector()
        self.neo4j = Neo4jManager()

    def run_leiden(self, resolution=1.0):
        # Runs Leiden algorithm and updates Neo4j with community IDs
        
        # Build Weighted Graph
        logger.info("Building weighted graph...")
        nx_graph = self.projector.build_weighted_graph()
        
        if nx_graph.number_of_nodes() == 0:
            logger.warning("Graph is empty.")
            return

        # Convert to iGraph (required by leidenalg)
        logger.info("Converting to iGraph...")
        ig_graph = ig.Graph.from_networkx(nx_graph)
        
        # Default weight to 1.0 if missing
        if "weight" not in ig_graph.edge_attributes():
            ig_graph.es["weight"] = [1.0] * ig_graph.ecount()

        # Run Leiden Algorithm
        logger.info(f"Running Leiden algorithm (resolution={resolution})...")
        partition = leidenalg.find_partition(
            ig_graph, 
            leidenalg.RBConfigurationVertexPartition, 
            weights=ig_graph.es["weight"],
            resolution_parameter=resolution
        )
        
        logger.info(f"Detected {len(partition)} communities.")

        # Write Results to Neo4j
        logger.info("Writing communities to Neo4j...")
        
        updates = []
        for community_id, nodes in enumerate(partition):
            for node_index in nodes:
                # Get original Tool ID from iGraph node
                tool_id = ig_graph.vs[node_index]["_nx_name"]
                updates.append({"tool_id": tool_id, "community_id": community_id})

        # Batch update 'communityId' on Tool nodes
        query = """
        UNWIND $batch AS row
        MATCH (t:Tool {id: row.tool_id})
        SET t.communityId = row.community_id
        """
        self.neo4j.execute_batch(query, updates)
        logger.info("Community detection completed.")

    def close(self):
        self.projector.close()
        self.neo4j.close()

if __name__ == "__main__":
    detector = LeidenDetector()
    try:
        detector.run_leiden()
    finally:
        detector.close()
