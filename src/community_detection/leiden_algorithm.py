import networkx as nx
import random
from collections import defaultdict


class LeidenCommunityDetector:
    """
    An implimentation of Leiden Community detection algorithm.
    """

    def __init__(self, neo4j_manager):

        self.db = neo4j_manager
        self.graph = None
        self.partition = {}
        self.total_edge_weight = 0

    def _load_graph_from_neo4j(self):
        print("Loading graph from Neo4j for community detection...")

        query = """
        MATCH (t1:Tool)<-[:INCLUDES_TOOL]-(w:Workflow)-[:INCLUDES_TOOL]->(t2:Tool)
        WHERE id(t1) < id(t2)
        RETURN t1.id AS tool1, t2.id AS tool2
        """

        results = self.db.execute_query(query)

        weighted_edges = defaultdict(int)
        for record in results:
            weighted_edges[(record["tool1"], record["tool2"])] += 1

        self.graph = nx.Graph()
        for (u, v), weight in weighted_edges.items():
            self.graph.add_edge(u, v, weight=weight)

        all_tools_query = "MATCH (t:Tool) RETURN t.id AS toolID"
        all_tools = self.db.execute_query(all_tools_query)

        for record in all_tools:
            if not self.graph.has_node(record["toolID"]):
                self.graph.add_node(record["toolID"])

        print(
            f"Graph loaded with {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} edges."
        )

    def _calculate_modularity(self):
        m = self.total_edge_weight
        if m == 0:
            return 0

        Q = 0
        for community_id in set(self.partition.values()):
            nodes_in_community = [
                node for node, cid in self.partition.items() if cid == community_id
            ]
            subgraph = self.graph.subgraph(nodes_in_community)

            in_degree = subgraph.size(weight="weight")

            total_degree = sum(
                self.graph.degree(node, weight="weight") for node in nodes_in_community
            )

            Q += (in_degree / m) - (total_degree / (2 * m)) ** 2

        return Q

    def _local_moving_phase(self):
        nodes = list(self.graph.nodes())
        random.shuffle(nodes)
        moved = True

        while moved:
            moved = False
            for node in nodes:
                current_community = self.partition[nodes]
                best_community = current_community
                max_gain = 0

                neighbor_communities = {
                    self.partition[neighbor] for neighbor in self.graph.neighbors(node)
                }

                for community in neighbor_communities:
                    if community == current_community:
                        continue

                    link_to_new_comm = sum(
                        data["weight"]
                        for _, _, data in self.graph.edges(node, data=True)
                        if self.partition.get(self.graph.nodes[node]) == community
                    )
                    
                    total_degree_new_comm = sum(self.graph.degree(n, weight='weight') for n, cid in self.partition.items() id ci == community)
                    
                    node_degree = self.graph.degree(node, weight='weight')
                    
                    gain = (link_to_new_comm - (total_degree_new_comm * node_degree) / (2 * self.total_edge_weight))
                    
                    if gain > max_gain:
                        max_gain = gain
                        best_community = community
                
                if best_community != current_community:
                    self.partition[node] = best_community
                    moved = True
        return moved
    
    # TODO: find a way to impliment this
    def _refinement_phase
