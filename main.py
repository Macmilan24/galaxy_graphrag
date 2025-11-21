# main.py
import json
from config import settings
from src.utils.logger import get_logger
from src.data_extraction.galaxy_extractor import GalaxyExtractor

logger = get_logger(__name__)

def run_data_extraction():
    """
    Runs the Galaxy extractor for both tools and workflows.
    """
    logger.info("Initializing Galaxy Extractor...")
    if not settings.GALAXY_URL:
        logger.error("GALAXY_URL is not set in the .env file. Cannot proceed.")
        return

    extractor = GalaxyExtractor(
        galaxy_url=settings.GALAXY_URL,
        galaxy_api_key=settings.GALAXY_API_KEY
    )
    

    tools = extractor.extract_tool()
    if tools:
        logger.info(f"Successfully extracted {len(tools)} tools.")
        print("\n--- Sample of Extracted Tools ---")
        for tool in tools[:3]:
            tool_dict = tool.__dict__
            tool_dict['help_text'] = (tool_dict.get('help_text') or '')[:150] + "..."
            print(json.dumps(tool_dict, indent=2))
        
        with open("data/tools.json", "w") as f:
            json.dump([t.__dict__ for t in tools], f, indent=4)
        logger.info("Saved tool data to data/tools.json")
    else:
        logger.warning("No tools were extracted.")
    

    workflows = extractor.extract_workflows()
    if workflows:
        logger.info(f"Successfully extracted {len(workflows)} workflows.")
        print("\n--- Sample of Extracted Workflows ---")
        for workflow in workflows[:3]:
            print(json.dumps(workflow.__dict__, indent=2, default=lambda o: o.__dict__))

        with open("data/workflows.json", "w") as f:
            json.dump([w.__dict__ for w in workflows], f, indent=4, default=lambda o: o.__dict__)
        logger.info("Saved workflow data to data/workflows.json")
    else:
        logger.warning("No workflows were extracted. Ensure GALAXY_API_KEY is correct.")

if __name__ == "__main__":
    logger.info("--- Starting Galaxy GraphRAG Pipeline: Data Extraction Phase ---")
    
    run_data_extraction()
    
    logger.info("--- Data Extraction Phase Finished ---")