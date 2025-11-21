import json
from src.graph_db.neo4j_manager import Neo4jManager
from src.utils.embeddings import EmbeddingService
from src.utils.logger import get_logger
from tqdm import tqdm

logger = get_logger("graph_builder")

class GraphBuilder:
    def __init__(self):
        self.neo4j = Neo4jManager()
        self.embedder = EmbeddingService()

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
        
        # Create Vector Index for Tools
        try:
            self.neo4j.execute_query("""
            CREATE VECTOR INDEX tool_embeddings IF NOT EXISTS
            FOR (t:Tool)
            ON (t.embedding)
            OPTIONS {indexConfig: {
             `vector.dimensions`: 384,
             `vector.similarity_function`: 'cosine'
            }}
            """)
            logger.info("Created vector index for Tools.")

            # Create Vector Index for Workflows
            self.neo4j.execute_query("DROP INDEX workflow_embeddings IF EXISTS")
            self.neo4j.execute_query("""
            CREATE VECTOR INDEX workflow_embeddings IF NOT EXISTS
            FOR (w:Workflow)
            ON (w.embedding)
            OPTIONS {indexConfig: {
             `vector.dimensions`: 384,
             `vector.similarity_function`: 'cosine'
            }}
            """)
            logger.info("Created vector index for Workflows.")
        except Exception as e:
            logger.warning(f"Could not create vector index (might be already existing or version mismatch): {e}")

    def load_tools(self, tools_file):
        """Loads tools from JSON and creates nodes/relationships."""
        logger.info(f"Loading tools from {tools_file}...")
        with open(tools_file, "r", encoding="utf-8") as f:
            tools = json.load(f)

        # Generate Embeddings
        logger.info("Generating embeddings for tools...")
        for tool in tqdm(tools, desc="Embedding Tools"):
            text_to_embed = f"{tool.get('name', '')} {tool.get('description', '')} {tool.get('help', '')[:500]}"
            tool['embedding'] = self.embedder.generate_embedding(text_to_embed)

        # Create Tools
        query_tool = """
        UNWIND $batch AS row
        MERGE (t:Tool {id: row.tool_id})
        SET t.name = row.name,
            t.description = row.description,
            t.version = row.version,
            t.help_text = row.help,
            t.embedding = row.embedding
        """
        self.neo4j.execute_batch(query_tool, tools)

        # Create Categories and Relationships
        query_category = """
        UNWIND $batch AS row
        MATCH (t:Tool {id: row.tool_id})
        UNWIND row.categories AS cat_name
        MERGE (c:Category {name: cat_name})
        MERGE (t)-[:BELONGS_TO]->(c)
        """
        self.neo4j.execute_batch(query_category, tools)

        # Create FileFormats and Relationships (Input/Output)
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

        # Generate Embeddings for Workflows
        logger.info("Generating embeddings for workflows...")
        for wf in tqdm(workflows, desc="Embedding Workflows"):
            text_to_embed = f"{wf.get('name', '')}"
            wf['embedding'] = self.embedder.generate_embedding(text_to_embed)

        # Create Workflows
        query_workflow = """
        UNWIND $batch AS row
        MERGE (w:Workflow {id: row.id})
        SET w.name = row.name,
            w.num_steps = row.number_of_steps,
            w.embedding = row.embedding
        """
        self.neo4j.execute_batch(query_workflow, workflows)

        # Create WorkflowSteps
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
        
        # Link Steps (NEXT_STEP)
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
