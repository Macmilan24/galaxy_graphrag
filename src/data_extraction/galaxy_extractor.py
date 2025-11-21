import os
import json
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from bioblend.galaxy import GalaxyInstance
from dotenv import load_dotenv
from jsonschema import validate, ValidationError
from tqdm import tqdm

from config.settings import GITHUB_TOKEN, GALAXY_URL, GALAXY_API_KEY
from src.utils.logger import get_logger

logger = get_logger("galaxy_data_extractor")

# Fix headers for GitHub API usage
GITHUB_HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"} if GITHUB_TOKEN else {}

# Galaxy config (already imported)
GALAXY_URL = GALAXY_URL
GALAXY_API_KEY = GALAXY_API_KEY

if not GALAXY_API_KEY:
    raise ValueError("GALAXY_API_KEY must be set in the .env file")

OUTPUT_FILE = "data/tools.json"
MAX_TOOLS = 500
MAX_RETRIES = 5
BASE_DELAY = 1.0

# IWC GitHub constants
GITHUB_API_URL = "https://api.github.com/repos/galaxyproject/iwc/contents/workflows"
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/galaxyproject/iwc/main"
OUTPUT_WORKFLOWS_FILE = "data/iwc_workflows.json"
OUTPUT_WORKFLOW_STEPS_FILE = "data/iwc_workflow_steps.json"
MAX_WORKFLOWS = 500

TOOL_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Galaxy Tool Metadata Schema",
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "tool_id": {"type": "string"},
            "name": {"type": "string", "minLength": 1},
            "description": {"type": "string"},
            "categories": {
                "type": "array",
                "items": {"type": ["string", "null"]},
            },
            "version": {"type": "string"},
            "help": {"type": ["string", "null"]},
            "input_formats": {"type": "array", "items": {"type": "string"}},
            "output_formats": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["tool_id", "name", "description", "categories", "version", "help"],
    },
}

# Schemas for Neo4j nodes
WORKFLOW_NODE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "IWC Workflow Node Schema",
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "name": {"type": "string"},
            "number_of_steps": {"type": "integer", "minimum": 0},
        },
        "required": ["id", "name", "number_of_steps"],
    },
}

WORKFLOW_STEP_NODE_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "IWC WorkflowStep Node Schema",
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "step_id": {"type": "string"},
            "step_number": {"type": ["integer", "string"]},
            "workflow_id": {"type": "string"},
            "tool_id": {"type": ["string", "null"]},
        },
        "required": ["step_id", "step_number", "workflow_id"],
    },
}

# -------------------------
# 2. HELPER FUNCTIONS
# -------------------------


def clean_help_text(text: str) -> str:
    """Removes HTML, markdown, and artifacts from help text."""
    if not isinstance(text, str):
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[=_]{2,}", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_tool_id(tool_id: str) -> str:
    """Removes version suffix from tool ID."""
    if not tool_id or "/" not in tool_id:
        return tool_id
    parts = tool_id.split("/")
    return "/".join(parts[:-1])


# ---- NEW: extract input/output formats ----
def extract_formats(root: ET.Element) -> tuple[list, list]:
    """Extract ONLY input formats + output formats."""
    input_formats = []
    output_formats = []

    # INPUTS
    inputs = root.find("inputs")
    if inputs is not None:
        for param in inputs.findall(".//param"):
            fmt = param.get("format")
            if fmt:
                for f in fmt.split(","):
                    f = f.strip()
                    if f and f not in input_formats:
                        input_formats.append(f)

    # OUTPUTS
    outputs = root.find("outputs")
    if outputs is not None:
        for data in outputs.findall(".//data"):
            fmt = data.get("format")
            if fmt:
                for f in fmt.split(","):
                    f = f.strip()
                    if f and f not in output_formats:
                        output_formats.append(f)

    return input_formats, output_formats


def get_with_retry(url: str, params: dict) -> requests.Response | None:
    """GET with retry & backoff for 429 / transient HTTP errors."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                logger.info(f"Request failed (final): {e}")
                return None
            time.sleep(BASE_DELAY * (2**attempt))
            continue

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else BASE_DELAY * (2**attempt)
            time.sleep(delay)
            continue

        if resp.ok:
            return resp

        if 500 <= resp.status_code < 600:
            # server error, backoff and retry
            if attempt < MAX_RETRIES - 1:
                time.sleep(BASE_DELAY * (2**attempt))
                continue

        # Non-retriable
        logger.info(f"Non-success HTTP {resp.status_code} for {url}")
        return None
    return None


# --- GitHub helpers (IWC workflows) ---


def github_get_with_retry(url: str) -> requests.Response | None:
    """GET against GitHub API with retry/backoff for 429/403/5xx."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=GITHUB_HEADERS, timeout=30)
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                logger.info(f"GitHub request failed (final): {e}")
                return None
            time.sleep(BASE_DELAY * (2**attempt))
            continue

        if resp.status_code in (429, 403):
            # Handle GitHub secondary rate limit or normal rate limit
            delay = None
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else BASE_DELAY * (2**attempt)
            else:
                remaining = resp.headers.get("X-RateLimit-Remaining")
                if remaining == "0":
                    reset = resp.headers.get("X-RateLimit-Reset")
                    if reset:
                        delay = max(1, int(reset) - int(time.time()) + 1)
            if delay is None:
                delay = BASE_DELAY * (2**attempt)
            time.sleep(delay)
            continue

        if resp.ok:
            return resp

        if 500 <= resp.status_code < 600:
            if attempt < MAX_RETRIES - 1:
                time.sleep(BASE_DELAY * (2**attempt))
                continue

        logger.info(f"GitHub HTTP {resp.status_code} for {url}")
        return None
    return None


def github_fetch_text(path_or_url: str) -> str | None:
    """Fetch text from a raw URL or repo-relative path under IWC main."""
    if not isinstance(path_or_url, str):
        return None
    url = (
        path_or_url
        if path_or_url.startswith("http")
        else f"{GITHUB_RAW_BASE}/{path_or_url.lstrip('/')}"
    )
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=GITHUB_HEADERS, timeout=30)
            if resp.status_code in (429, 403):
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else BASE_DELAY * (2**attempt)
                time.sleep(delay)
                continue
            if resp.ok:
                return resp.text
            if 500 <= resp.status_code < 600:
                time.sleep(BASE_DELAY * (2**attempt))
                continue
            logger.info(f"GitHub raw fetch failed {resp.status_code} for {url}")
            return None
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                logger.info(f"GitHub raw fetch failed (final): {e}")
                return None
            time.sleep(BASE_DELAY * (2**attempt))
    return None


def parse_ga_steps(ga_text: str) -> tuple[str, list[dict]]:
    """Return (workflow_name, steps_list_from_ga)."""
    try:
        data = json.loads(ga_text)
        name = data.get("name") or ""
        steps = list((data.get("steps") or {}).items())  # [(key, step_dict), ...]
        return name, steps
    except json.JSONDecodeError:
        return "", []


def process_iwc_repo(category: str, repo: dict) -> tuple[dict | None, list[dict]]:
    """
    Produce a Workflow node and a list of WorkflowStep nodes for a single repo.
    - Workflow: { id, name, number_of_steps }
    - WorkflowStep: { step_id, step_number, workflow_id }
    """
    repo_name = repo.get("name", "").strip()
    if not repo_name:
        return None, []

    # Get files inside this repo directory
    contents_resp = github_get_with_retry(repo.get("url"))
    if not contents_resp:
        return None, []
    contents = contents_resp.json()

    # Find first .ga file
    ga_item = next(
        (
            i
            for i in contents
            if i.get("type") == "file" and i.get("name", "").lower().endswith(".ga")
        ),
        None,
    )
    if not ga_item:
        return None, []

    ga_source = ga_item.get("download_url") or ga_item.get("path")
    ga_text = github_fetch_text(ga_source)
    if not ga_text:
        return None, []

    wf_name_from_ga, steps = parse_ga_steps(ga_text)
    workflow_id = f"iwc_{category.lower()}_{repo_name.lower()}"
    workflow_name = (wf_name_from_ga or repo_name).strip()
    number_of_steps = len(steps)

    workflow_node = {
        "id": workflow_id,
        "name": workflow_name,
        "number_of_steps": number_of_steps,
    }

    step_nodes: list[dict] = []
    for key, step in steps:
        # step_number: prefer numeric key; fallback to step['id']
        try:
            step_number = int(key)
        except Exception:
            step_number = step.get("id", key)
        step_uid = f"{workflow_id}_{step_number}"
        
        # Extract tool_id if present
        tool_id = step.get("tool_id") or step.get("content_id")
        
        step_nodes.append(
            {
                "step_id": step_uid,
                "step_number": step_number,
                "workflow_id": workflow_id,
                "tool_id": tool_id,
            }
        )

    return workflow_node, step_nodes


def validate_workflow_nodes(data: list[dict]) -> bool:
    try:
        validate(instance=data, schema=WORKFLOW_NODE_SCHEMA)
        return True
    except ValidationError as e:
        logger.info(f"Workflow node validation failed: {e.message}")
        return False


def validate_workflow_step_nodes(data: list[dict]) -> bool:
    try:
        validate(instance=data, schema=WORKFLOW_STEP_NODE_SCHEMA)
        return True
    except ValidationError as e:
        logger.info(f"WorkflowStep node validation failed: {e.message}")
        return False


def validate_data(data: list[dict]) -> bool:
    logger.info("Validating final data...")
    try:
        validate(instance=data, schema=TOOL_SCHEMA)
        logger.info("Validation successful!")
        return True
    except ValidationError as e:
        logger.info(f"Validation FAILED: {e.message}")
        return False


# -------------------------
# 3. MAIN PIPELINE
# -------------------------


def main():
    start_time = datetime.now()
    logger.info(f"Starting tool pipeline at {start_time}")

    gi = GalaxyInstance(url=GALAXY_URL, key=GALAXY_API_KEY)

    logger.info("Fetching list of tools...")
    try:
        all_tools = gi.tools.get_tools()
        logger.info(f"Found {len(all_tools)} tools total.")
    except Exception as e:
        logger.info(f"Failed to fetch tools: {e}")
        return

    # Limit to first MAX_TOOLS
    tools_subset = all_tools[:MAX_TOOLS]
    logger.info(f"Processing first {len(tools_subset)} tools (cap={MAX_TOOLS}).")

    processed_tools = []
    for tool in tqdm(tools_subset, desc="Processing tools", unit="tool"):
        result = fetch_and_process_tool(tool, gi)
        if result:
            processed_tools.append(result)

    if not processed_tools:
        logger.info("No tools processed. Exiting.")
        return

    logger.info(f"Processed {len(processed_tools)} tools.")

    if not validate_data(processed_tools):
        logger.info("Aborting due to validation failure.")
        return

    out_dir = os.path.dirname(OUTPUT_FILE)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(processed_tools, f, indent=2, ensure_ascii=False)

    duration = datetime.now() - start_time
    logger.info(f"Pipeline completed in {duration}")
    logger.info(f"Output file: {OUTPUT_FILE}")

    # --- IWC workflow extraction (capped at MAX_WORKFLOWS) ---
    logger.info("Starting IWC workflow extraction...")
    workflows: list[dict] = []
    workflow_steps: list[dict] = []
    processed = 0

    # List top-level workflow categories
    cats_resp = github_get_with_retry(GITHUB_API_URL)
    if not cats_resp:
        logger.info("Failed to list IWC workflow categories.")
        return
    categories = [c for c in cats_resp.json() if c.get("type") == "dir"]

    for cat in tqdm(categories, desc="IWC categories", unit="cat"):
        if processed >= MAX_WORKFLOWS:
            break
        # List repos inside category
        repos_resp = github_get_with_retry(cat.get("url"))
        if not repos_resp:
            continue
        repos = [r for r in repos_resp.json() if r.get("type") == "dir"]
        for repo in tqdm(
            repos, desc=f"{cat.get('name','category')}", leave=False, unit="repo"
        ):
            if processed >= MAX_WORKFLOWS:
                break
            wf_node, steps_nodes = process_iwc_repo(cat.get("name", ""), repo)
            if wf_node:
                workflows.append(wf_node)
                workflow_steps.extend(steps_nodes)
                processed += 1

    logger.info(f"IWC workflows processed: {len(workflows)}")

    if not workflows:
        logger.info("No IWC workflows processed. Exiting.")
        return

    if not validate_workflow_nodes(workflows) or not validate_workflow_step_nodes(
        workflow_steps
    ):
        logger.info("Aborting IWC workflow write due to validation failure.")
        return

    os.makedirs(os.path.dirname(OUTPUT_WORKFLOWS_FILE), exist_ok=True)
    with open(OUTPUT_WORKFLOWS_FILE, "w", encoding="utf-8") as f:
        json.dump(workflows, f, indent=2, ensure_ascii=False)
    with open(OUTPUT_WORKFLOW_STEPS_FILE, "w", encoding="utf-8") as f:
        json.dump(workflow_steps, f, indent=2, ensure_ascii=False)

    logger.info(f"Wrote workflows to: {OUTPUT_WORKFLOWS_FILE}")
    logger.info(f"Wrote workflow steps to: {OUTPUT_WORKFLOW_STEPS_FILE}")


def fetch_and_process_tool(tool: dict, gi: GalaxyInstance) -> dict | None:
    """Fetch raw XML for a Galaxy tool and return cleaned metadata."""
    tool_id = tool.get("id", "")
    if not tool_id:
        return None

    raw_tool_url = f"{GALAXY_URL}/api/tools/{tool_id}/raw_tool_source"
    resp = get_with_retry(raw_tool_url, params={"key": GALAXY_API_KEY})
    if resp is None:
        logger.info(f"Giving up on tool {tool_id}")
        return None

    try:
        root = ET.fromstring(resp.text)
        help_elem = root.find("help")
        help_text = (
            help_elem.text.strip() if (help_elem is not None and help_elem.text) else ""
        )

        input_formats, output_formats = extract_formats(root)

        return {
            "tool_id": tool_id,
            "name": tool.get("name", ""),
            "description": tool.get("description", ""),
            "categories": [tool.get("panel_section_name", "Uncategorized")],
            "version": tool.get("version", ""),
            "help": clean_help_text(help_text),
            "input_formats": input_formats,
            "output_formats": output_formats,
        }
    except Exception as e:
        logger.info(f"Error parsing tool {tool_id}: {e}")
        return None


if __name__ == "__main__":
    main()
