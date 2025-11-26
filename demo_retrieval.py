from src.retrieval.search import GlobalSearch, LocalSearch, HybridSearch
from src.utils.logger import get_logger

logger = get_logger("demo_retrieval")

def run_demo():
    print("="*50)
    print("Galaxy GraphRAG Retrieval Demo")
    print("="*50)

    # --- 1. Global Search ---
    print("\n[1] Testing Global Search (Community-Centric)...")
    try:
        global_search = GlobalSearch()
        query = "What tools are available for Variant Calling?"
        print(f"Query: {query}")
        result = global_search.search(query)
        print(f"Result:\n{result}")
    except Exception as e:
        logger.error(f"Global Search failed: {e}")

    # --- 2. Local Search ---
    print("\n" + "="*50)
    print("[2] Testing Local Search (Entity-Centric)...")
    try:
        local_search = LocalSearch()
        query = "fastqc" 
        print(f"Query: {query}")
        results = local_search.search(query, top_k=2)
        
        for i, res in enumerate(results):
            tool = res['tool']
            context = res['context']
            print(f"\n  Result {i+1}: {tool['name']}")
            print(f"  Score: {tool['score']:.4f}")
            print(f"  Description: {tool['description'][:100]}...")
            print(f"  Inputs: {context['inputs']}")
            print(f"  Outputs: {context['outputs']}")
            print(f"  Used in Workflows: {context['workflows']}")
    except Exception as e:
        logger.error(f"Local Search failed: {e}")

    # --- 3. Hybrid Search ---
    print("\n" + "="*50)
    print("[3] Testing Hybrid Search (Vector + Graph Filter)...")
    try:
        hybrid_search = HybridSearch()
        query = "alignment"
        input_fmt = "fastq"
        print(f"Query: '{query}' | Filter: Input must contain '{input_fmt}'")
        
        results = hybrid_search.search(query, input_format=input_fmt, top_k=3)
        
        for i, res in enumerate(results):
            print(f"\n  Result {i+1}: {res['name']}")
            print(f"  Score: {res['score']:.4f}")
            print(f"  Description: {res['description'][:100]}...")
    except Exception as e:
        logger.error(f"Hybrid Search failed: {e}")

if __name__ == "__main__":
    run_demo()
