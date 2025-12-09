from src.graph_db.neo4j_manager import Neo4jManager

def verify_hierarchy():
    neo4j = Neo4jManager()
    
    print("="*60)
    print("HIERARCHICAL COMMUNITY VERIFICATION")
    print("="*60)
    
    # 1. Count Communities and SubCommunities
    query_counts = """
    MATCH (c:Community)
    OPTIONAL MATCH (c)<-[:BELONGS_TO]-(s:SubCommunity)
    RETURN count(DISTINCT c) as communities, count(DISTINCT s) as subcommunities
    """
    counts = neo4j.execute_query(query_counts)[0]
    print(f"Total Level 1 Communities: {counts['communities']}")
    print(f"Total Level 2 SubCommunities: {counts['subcommunities']}")
    print("-" * 60)

    # 2. List Top 5 Communities by size
    query_comms = """
    MATCH (c:Community)
    OPTIONAL MATCH (node)-[:IN_COMMUNITY]->(c)
    RETURN c.id as id, c.name as title, c.summary as summary, count(node) as size
    ORDER BY size DESC
    LIMIT 5
    """
    communities = neo4j.execute_query(query_comms)
    
    for comm in communities:
        print(f"\n[L1] Community {comm['id']}: {comm['title']} (Size: {comm['size']})")
        print(f"     Summary: {comm['summary'][:100]}...")
        
        # Get SubCommunities in this Community
        query_subs = """
        MATCH (s:SubCommunity)-[:BELONGS_TO]->(c:Community {id: $cid})
        OPTIONAL MATCH (n)-[:IN_SUBCOMMUNITY]->(s)
        RETURN s.id as id, s.name as title, count(n) as size, labels(head(collect(n)))[0] as type
        ORDER BY size DESC
        """
        subs = neo4j.execute_query(query_subs, {"cid": comm['id']})
        
        for sub in subs:
            print(f"    -> [L2] SubCommunity {sub['id']} ({sub['type']}): {sub['title']} (Size: {sub['size']})")
            
            # Sample a few nodes
            query_sample = """
            MATCH (n)-[:IN_SUBCOMMUNITY]->(s:SubCommunity {id: $sid})
            RETURN n.name as name
            LIMIT 3
            """
            samples = neo4j.execute_query(query_sample, {"sid": sub['id']})
            sample_names = [r['name'] for r in samples]
            print(f"       Ex: {', '.join(sample_names)}")

    neo4j.close()

if __name__ == "__main__":
    verify_hierarchy()
