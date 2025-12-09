import leidenalg
import igraph as ig
import networkx as nx
from src.community_detection.universal_projector import UniversalGraphProjector
from src.community_detection.graph_projector import GraphProjector
from src.graph_db.neo4j_manager import Neo4jManager
from src.utils.logger import get_logger

logger = get_logger("hierarchical_leiden")

class HierarchicalLeiden:
    """
    Implements the 2-Level Heterogeneous Community Detection.
    Level 1: Universal Semantic Topics (Tools + Workflows).
    Level 2: Functional Sub-Communities (Tools only, Workflows only).
    """
    def __init__(self):
        self.universal_projector = UniversalGraphProjector()
        self.tool_projector = GraphProjector()
        self.neo4j = Neo4jManager()

    def _run_leiden_on_graph(self, nx_graph, resolution=1.0):
        """Helper to run Leiden on a NetworkX graph."""
        if nx_graph.number_of_nodes() == 0:
            return {}
            
        ig_graph = ig.Graph.from_networkx(nx_graph)
        if "weight" not in ig_graph.edge_attributes():
            ig_graph.es["weight"] = [1.0] * ig_graph.ecount()
            
        partition = leidenalg.find_partition(
            ig_graph, 
            leidenalg.RBConfigurationVertexPartition, 
            weights=ig_graph.es["weight"],
            resolution_parameter=resolution
        )
        
        # Map: node_id -> community_id
        results = {}
        for comm_id, nodes in enumerate(partition):
            for node_idx in nodes:
                original_id = ig_graph.vs[node_idx]["_nx_name"]
                results[original_id] = comm_id
        return results

    def run_hierarchical_detection(self):
        logger.info("Starting Hierarchical Community Detection...")
        
        # --- Cleanup: Remove existing communities ---
        logger.info("Cleaning up existing community nodes...")
        cleanup_query = """
        MATCH (n) WHERE n:Community OR n:SubCommunity OR n:Topic
        DETACH DELETE n
        """
        self.neo4j.execute_query(cleanup_query)
        
        # Also remove properties from Tool/Workflow
        cleanup_props = """
        MATCH (n) WHERE n:Tool OR n:Workflow
        REMOVE n.communityId, n.subCommunityId, n.topicId
        """
        self.neo4j.execute_query(cleanup_props)

        # --- Level 1: Universal Semantic Layer ---
        logger.info("[Level 1] Building Universal Graph...")
        univ_graph, node_types = self.universal_projector.build_universal_graph()
        
        logger.info("[Level 1] Running Leiden...")
        # Use lower resolution for broader "Communities"
        comm_partition = self._run_leiden_on_graph(univ_graph, resolution=1.0)
        
        # Group nodes by Community
        communities = {} # {comm_id: {'tools': [], 'workflows': []}}
        for node_key, comm_id in comm_partition.items():
            if comm_id not in communities:
                communities[comm_id] = {'tools': [], 'workflows': []}
            
            # Parse key "Type:ID"
            type_prefix, real_id = node_key.split(":", 1)
            if type_prefix == "Tool":
                communities[comm_id]['tools'].append(real_id)
            elif type_prefix == "Workflow":
                communities[comm_id]['workflows'].append(real_id)

        logger.info(f"[Level 1] Found {len(communities)} Communities.")

        # --- Level 2: Local Refinement ---
        all_updates = []
        
        for comm_id, members in communities.items():
            logger.info(f"Processing Community {comm_id} ({len(members['tools'])} tools, {len(members['workflows'])} workflows)...")
            
            # 1. Tool Sub-Communities
            tool_ids = members['tools']
            if tool_ids:
                # Use existing GraphProjector with filter
                tool_graph = self.tool_projector.build_weighted_graph(filter_tool_ids=tool_ids)
                tool_partition = self._run_leiden_on_graph(tool_graph, resolution=1.2) # Higher res for finer grain
                
                for tid, sub_id in tool_partition.items():
                    all_updates.append({
                        "type": "Tool",
                        "id": tid,
                        "comm_id": comm_id,
                        "sub_id": f"{comm_id}_T_{sub_id}" # Unique SubCommunity ID
                    })

            # 2. Workflow Sub-Communities 
            wf_ids = members['workflows']
            if wf_ids:
                # Create subgraph from Universal graph for just these workflows
                wf_keys = [f"Workflow:{wid}" for wid in wf_ids]
                wf_subgraph = univ_graph.subgraph(wf_keys)
                wf_partition = self._run_leiden_on_graph(wf_subgraph, resolution=1.0)
                
                for key, sub_id in wf_partition.items():
                    wid = key.split(":")[1]
                    all_updates.append({
                        "type": "Workflow",
                        "id": wid,
                        "comm_id": comm_id,
                        "sub_id": f"{comm_id}_W_{sub_id}"
                    })

        # Save to Neo4j 
        logger.info("Writing Hierarchical Communities to Neo4j...")
        
        tools_batch = [r for r in all_updates if r['type'] == 'Tool']
        wf_batch = [r for r in all_updates if r['type'] == 'Workflow']
        
        query_tool = """
        UNWIND $batch AS row
        MATCH (t:Tool {id: row.id})
        SET t.communityId = row.comm_id,
            t.subCommunityId = row.sub_id
        MERGE (c:Community {id: row.comm_id})
        MERGE (sub:SubCommunity {id: row.sub_id})
        MERGE (t)-[:IN_COMMUNITY]->(c)
        MERGE (t)-[:IN_SUBCOMMUNITY]->(sub)
        MERGE (sub)-[:BELONGS_TO]->(c)
        """
        
        query_wf = """
        UNWIND $batch AS row
        MATCH (w:Workflow {id: row.id})
        SET w.communityId = row.comm_id,
            w.subCommunityId = row.sub_id
        MERGE (c:Community {id: row.comm_id})
        MERGE (sub:SubCommunity {id: row.sub_id})
        MERGE (w)-[:IN_COMMUNITY]->(c)
        MERGE (w)-[:IN_SUBCOMMUNITY]->(sub)
        MERGE (sub)-[:BELONGS_TO]->(c)
        """

        if tools_batch:
            self.neo4j.execute_batch(query_tool, tools_batch)
        if wf_batch:
            self.neo4j.execute_batch(query_wf, wf_batch)
            
        logger.info("Hierarchical detection complete.")

    def close(self):
        self.universal_projector.close()
        self.tool_projector.close()
        self.neo4j.close()

if __name__ == "__main__":
    detector = HierarchicalLeiden()
    try:
        detector.run_hierarchical_detection()
    finally:
        detector.close()
