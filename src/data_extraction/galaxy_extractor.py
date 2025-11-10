# src/data_extraction/galaxy_extractor.py

import os
import json
from bioblend.galaxy import GalaxyInstance
from config import settings
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


TOOL_LIMIT = 1000
WORKFLOW_LIMIT = 1000
MAX_CONCURRENT_WORKERS = 10


class GalaxyExtractor:
    """
    Connects to a Galaxy instance to extract information concurrently.
    """

    def __init__(self, url, api_key):
        print("Initializing connection to Galaxy instance...")
        try:
            self.gi = GalaxyInstance(url=url, key=api_key)
            self.gi.users.get_current_user()
            print("Successfully connected to Galaxy.")
        except Exception as e:
            print(
                f"Error: Could not connect to Galaxy. Please check your URL and API Key. Details: {e}"
            )
            raise

    def _fetch_tool_details(self, summary_tool):
        """
        Fetches and processes details for a single tool.
        This is the target function for our concurrent workers.
        """
        tool_id = summary_tool.get("id")
        if not tool_id:
            return None

        try:
            tool_details = self.gi.tools.show_tool(tool_id, io_details=True)

            input_formats = set()
            for tool_input in tool_details.get("inputs", []):
                formats = tool_input.get("format", [])
                input_formats.update(
                    formats if isinstance(formats, list) else [formats]
                )

            output_formats = set(
                out.get("format", "unknown") for out in tool_details.get("outputs", [])
            )

            return {
                "id": tool_details.get("id"),
                "name": tool_details.get("name"),
                "description": tool_details.get("description"),
                "help_text": tool_details.get("help", ""),
                "input_formats": list(input_formats),
                "output_formats": list(output_formats),
                "category": summary_tool.get("panel_section_name", "Miscellaneous"),
            }
        except Exception:

            return None

    def _extract_tools_concurrently(self, limit=None):
        """
        Fetches tools concurrently using a ThreadPoolExecutor.
        """
        print(
            f"Starting concurrent tool extraction (Max Workers: {MAX_CONCURRENT_WORKERS})..."
        )

        summary_tools = [
            t for t in self.gi.tools.get_tools() if t.get("model_class") == "Tool"
        ]

        if limit:
            print(f"Applying limit: Fetching details for the first {limit} tools.")
            summary_tools = summary_tools[:limit]

        extracted_tools = []
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:

            future_to_tool = {
                executor.submit(self._fetch_tool_details, tool): tool
                for tool in summary_tools
            }

            for future in tqdm(
                as_completed(future_to_tool),
                total=len(summary_tools),
                desc="Fetching Tool Details",
            ):
                result = future.result()
                if result:
                    extracted_tools.append(result)

        print(f"Extracted detailed information for {len(extracted_tools)} tools.")
        return extracted_tools

    def _fetch_workflow_details(self, wf_summary):
        """
        Fetches and processes details for a single workflow.
        This is the target for the workflow thread pool workers.
        """
        wf_id = wf_summary.get("id")
        if not wf_id:
            return None
        try:
            wf_details = self.gi.workflows.show_workflow(wf_id)
            tool_ids = list(
                set(
                    step["tool_id"]
                    for step in wf_details.get("steps", {}).values()
                    if step.get("type") == "tool" and step.get("tool_id") is not None
                )
            )
            if tool_ids:
                return {
                    "id": wf_details["id"],
                    "name": wf_details["name"],
                    "num_steps": len(wf_details["steps"]),
                    "included_tools": tool_ids,
                }
            return None
        except Exception:
            return None

    def _extract_workflows(self, limit=None):
        print(
            f"Starting concurrent workflow extraction (Max Workers: {MAX_CONCURRENT_WORKERS})..."
        )
        summary_workflows = self.gi.workflows.get_workflows(published=True)
        if limit:
            print(f"Applying limit: Fetching details for the first {limit} workflows.")
            summary_workflows = summary_workflows[:limit]

        extracted_workflows = []
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_WORKERS) as executor:
            future_to_workflow = {
                executor.submit(self._fetch_workflow_details, wf): wf
                for wf in summary_workflows
            }
            for future in tqdm(
                as_completed(future_to_workflow),
                total=len(summary_workflows),
                desc="Fetching Workflow Details",
            ):
                result = future.result()
                if result:
                    extracted_workflows.append(result)

        print(f"Extracted details for {len(extracted_workflows)} workflows.")
        return extracted_workflows

    

    def extract_and_save(self, output_dir="data", tool_limit=None, workflow_limit=None):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory: {output_dir}")

        tools_data = self._extract_tools_concurrently(limit=tool_limit)
        workflows_data = self._extract_workflows(limit=workflow_limit)

        tools_path = os.path.join(output_dir, "tools.json")
        workflows_path = os.path.join(output_dir, "workflows.json")

        with open(tools_path, "w") as f:
            json.dump(tools_data, f, indent=4)
        print(f"Successfully saved tool data to {tools_path}")

        with open(workflows_path, "w") as f:
            json.dump(workflows_data, f, indent=4)
        print(f"Successfully saved workflow data to {workflows_path}")


if __name__ == "__main__":
    print("--- Starting Galaxy Data Extraction Pipeline ---")

    if not settings.GALAXY_URL or not settings.GALAXY_API_KEY:
        print("Error: GALAXY_URL and GALAXY_API_KEY must be set in your .env file.")
    else:
        try:
            extractor = GalaxyExtractor(
                url=settings.GALAXY_URL, api_key=settings.GALAXY_API_KEY
            )

            extractor.extract_and_save(
                tool_limit=TOOL_LIMIT, workflow_limit=WORKFLOW_LIMIT
            )
            print("--- Data Extraction Pipeline Finished Successfully ---")
        except Exception as e:
            print(f"--- Data Extraction Pipeline Failed. Error: {e} ---")
