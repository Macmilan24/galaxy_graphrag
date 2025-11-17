from src.data_extraction.galaxy_extractor import (
    GalaxyExtractor,
    TOOL_LIMIT,
    WORKFLOW_LIMIT,
)
from src.graph_db.neo4j_manager import Neo4jManager
from src.graph_db.graph_builder import GraphBuilder
from config import settings


def run_pipeline():
    """
    Executes the full data extraction and graph building pipeline.
    """
    # --- Step 1: Data Extraction ---
    print(">>> STEP 1: Starting Data Extraction <<<")
    try:
        extractor = GalaxyExtractor(
            url=settings.GALAXY_URL, api_key=settings.GALAXY_API_KEY
        )
        extractor.extract_and_save(tool_limit=TOOL_LIMIT, workflow_limit=WORKFLOW_LIMIT)
        print(">>> Data Extraction Finished Successfully <<<")
    except Exception as e:
        print(f">>> Data Extraction Failed. Aborting pipeline. Error: {e} <<<")
        return

    # --- Step 2: Graph Building ---
    db_manager = None
    try:
        print("\n>>> STEP 2: Starting Graph Building <<<")
        db_manager = Neo4jManager()
        builder = GraphBuilder(db_manager)

        # Prepare the database
        builder.create_constraints()

        # Build the graph
        builder.build_graph()

        print(">>> Graph Building Finished Successfully <<<")
    except Exception as e:
        print(f">>> Graph Building Failed. Error: {e} <<<")
    finally:
        # Ensure the database connection is always closed
        if db_manager:
            db_manager.close()


if __name__ == "__main__":
    run_pipeline()
