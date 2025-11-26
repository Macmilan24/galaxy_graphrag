import argparse
import sys
from src.graph_db.graph_builder import GraphBuilder
from src.community_detection.leiden import LeidenDetector
from src.community_detection.summarizer import CommunitySummarizer
from src.retrieval.search import GlobalSearch, LocalSearch, HybridSearch
from src.utils.logger import get_logger

logger = get_logger("main")

def build_pipeline():
    """
    Runs the complete data ingestion and graph construction pipeline.
    1. Build Graph (Nodes & Edges) + Generate Embeddings
    2. Detect Communities (Leiden)
    3. Summarize Communities (LLM)
    """
    print("\n" + "="*50)
    print("Starting Galaxy GraphRAG Build Pipeline")
    print("="*50)

    # 1. Build Graph & Embeddings
    print("\n[Step 1/3] Building Graph and Generating Embeddings...")
    builder = GraphBuilder()
    try:
        builder.build_full_graph()
        print("Graph construction complete.")
    except Exception as e:
        logger.error(f"Graph build failed: {e}")
        return
    finally:
        builder.close()

    # 2. Detect Communities
    print("\n[Step 2/3] Detecting Communities (Leiden)...")
    detector = LeidenDetector()
    try:
        detector.run_leiden()
        print("Community detection complete.")
    except Exception as e:
        logger.error(f"Community detection failed: {e}")
        return
    finally:
        detector.close()

    # 3. Summarize Communities
    print("\n[Step 3/3] Summarizing Communities (LLM)...")
    summarizer = CommunitySummarizer()
    try:
        summarizer.run_summarization()
        print("Community summarization complete.")
    except Exception as e:
        logger.error(f"Summarization failed: {e}")
        return
    finally:
        summarizer.close()

    print("\n" + "="*50)
    print("Pipeline Completed Successfully!")
    print("="*50)

def run_query(query, mode="auto"):
    """
    Executes a search query against the built graph.
    """
    print(f"\nQuery: {query}")
    print("-" * 30)

    if mode == "global":
        searcher = GlobalSearch()
        result = searcher.search(query)
        print(f"Global Search Result:\n{result}")
    
    elif mode == "local":
        searcher = LocalSearch()
        results = searcher.search(query)
        for i, res in enumerate(results):
            print(f"\nResult {i+1}: {res['tool']['name']} (Score: {res['tool']['score']:.4f})")
            print(f"Description: {res['tool']['description'][:150]}...")
            
    elif mode == "hybrid":
    
        searcher = HybridSearch()
        results = searcher.search(query) # No filter for now in simple CLI
        for i, res in enumerate(results):
            print(f"\nResult {i+1}: {res['name']} (Score: {res['score']:.4f})")
            
    else: 
        if len(query.split()) > 5:
            print("[Auto-Mode] Selected Global Search")
            run_query(query, mode="global")
        else:
            print("[Auto-Mode] Selected Local Search")
            run_query(query, mode="local")

def main():
    parser = argparse.ArgumentParser(description="Galaxy GraphRAG CLI")
    
    # Mode selection
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--build", action="store_true", help="Run the full graph build and indexing pipeline")
    group.add_argument("--query", type=str, help="Run a search query")
    
    parser.add_argument("--mode", type=str, choices=["global", "local", "hybrid", "auto"], default="auto", help="Search mode (default: auto)")

    args = parser.parse_args()

    if args.build:
        build_pipeline()
    elif args.query:
        run_query(args.query, args.mode)

if __name__ == "__main__":
    main()