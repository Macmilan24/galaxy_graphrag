import json
from src.graph_db.neo4j_manager import Neo4jManager
from src.utils.logger import get_logger

logger = get_logger("graph_builder")

class GraphBuilder:
    def __init__(self):
        self.neo4j = Neo4jManager()

    def clear_database(self):
        """Deletes all nodes and relationships."""
        logger.info("Clearing database...")
        self.neo4j.execute_query("MATCH (n) DETACH DELETE n")
        logger.info("Database cleared.")

    def create_indexes(self):
        """Creates constraints and indexes."""
        constraints = [
            "CREATE CONSTRAINT FOR (t:Tool) REQUIRE t.id IS UNIQUE",
            "CREATE CONSTRAINT FOR (w:Workflow) REQUIRE w.id IS UNIQUE",
            "CREATE CONSTRAINT FOR (ws:WorkflowStep) REQUIRE ws.step_id IS UNIQUE",
            "CREATE CONSTRAINT FOR (f:FileFormat) REQUIRE f.name IS UNIQUE",
            "CREATE CONSTRAINT FOR (c:Category) REQUIRE c.name IS UNIQUE",
        ]
        self.neo4j.create_constraints(constraints)

    def load_tools(self, tools_file):
        """Loads tools from JSON and creates nodes/relationships."""
        logger.info(f"Loading tools from {tools_file}...")
        with open(tools_file, "r", encoding="utf-8") as f:
            tools = json.load(f)

        # 1. Create Tools
        query_tool = """
        UNWIND $batch AS row
        MERGE (t:Tool {id: row.tool_id})
        SET t.name = row.name,
            t.description = row.description,
            t.version = row.version,
            t.help_text = row.help
        """
        self.neo4j.execute_batch(query_tool, tools)

        # 2. Create Categories and Relationships
        query_category = """
        UNWIND $batch AS row
        MATCH (t:Tool {id: row.tool_id})
        UNWIND row.categories AS cat_name
        MERGE (c:Category {name: cat_name})
        MERGE (t)-[:BELONGS_TO]->(c)
        """
        self.neo4j.execute_batch(query_category, tools)

        # 3. Create FileFormats and Relationships (Input/Output)
        query_formats = """
        UNWIND $batch AS row
        MATCH (t:Tool {id: row.tool_id})
        
        FOREACH (fmt IN row.input_formats | 
            MERGE (f:FileFormat {name: fmt})
            MERGE (t)-[:ACCEPTS_INPUT]->(f)
        )
        
        FOREACH (fmt IN row.output_formats | 
            MERGE (f:FileFormat {name: fmt})
            MERGE (t)-[:PRODUCES_OUTPUT]->(f)
        )
        """
        self.neo4j.execute_batch(query_formats, tools)
        logger.info("Tools loaded successfully.")

    def load_workflows(self, workflows_file, steps_file):
        """Loads workflows and steps."""
        logger.info(f"Loading workflows from {workflows_file}...")
        with open(workflows_file, "r", encoding="utf-8") as f:
            workflows = json.load(f)
        
        with open(steps_file, "r", encoding="utf-8") as f:
            steps = json.load(f)

        # 1. Create Workflows
        query_workflow = """
        UNWIND $batch AS row
        MERGE (w:Workflow {id: row.id})
        SET w.name = row.name,
            w.num_steps = row.number_of_steps
        """
        self.neo4j.execute_batch(query_workflow, workflows)

        # 2. Create WorkflowSteps
        query_steps = """
        UNWIND $batch AS row
        MERGE (ws:WorkflowStep {step_id: row.step_id})
        SET ws.step_number = row.step_number
        
        WITH ws, row
        MATCH (w:Workflow {id: row.workflow_id})
        MERGE (w)-[:HAS_STEP]->(ws)
        
        WITH ws, row
        WHERE row.tool_id IS NOT NULL
        MATCH (t:Tool {id: row.tool_id})
        MERGE (ws)-[:USES_TOOL]->(t)
        """
        self.neo4j.execute_batch(query_steps, steps)
        
        # 3. Link Steps (NEXT_STEP) - Simplified based on step number
        # This is a bit complex in batch, so we might do it per workflow or assume step_number order
        # For now, let's skip explicit NEXT_STEP unless we have connection data, 
        # or we can run a post-processing query to link steps by number.
        
        query_link_steps = """
        MATCH (w:Workflow)-[:HAS_STEP]->(ws1:WorkflowStep)
        MATCH (w)-[:HAS_STEP]->(ws2:WorkflowStep)
        WHERE ws2.step_number = ws1.step_number + 1
        MERGE (ws1)-[:NEXT_STEP]->(ws2)
        """
        self.neo4j.execute_query(query_link_steps)
        
        logger.info("Workflows loaded successfully.")

    def close(self):
        self.neo4j.close()

    def build_full_graph(self):
        self.clear_database()
        self.create_indexes()
        self.load_tools("data/tools.json")
        self.load_workflows("data/iwc_workflows.json", "data/iwc_workflow_steps.json")

if __name__ == "__main__":
    builder = GraphBuilder()
    try:
        builder.build_full_graph()
    finally:
        builder.close()
