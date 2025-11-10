import json
from .neo4j_manager import Neo4jManager
from src.utils.embedding_utils import get_embedding
from tqdm import tqdm

class GraphBuilder:
    def __init__(self, neo4j_manager: Neo4jManager):
        self.db = neo4j_manager
    
    def _load_data(self, filepath):
        print(f"Loading data from {filepath} ...")
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def create_constraints(self):
        print("creating database constraints ...")
        
        self.db.execute_query("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Tool) REQUIRE t.id IS UNIQUE")
        self.db.execute_query("CREATE CONSTRAINT IF NOT EXISTS FOR (w:Workflow) REQUIRE w.id IS UNIQUE")
        self.db.execute_query("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE")
        self.db.execute_query("CREATE CONSTRAINT IF NOT EXISTS FOR (f:FileFormat) REQUIRE f.id IS UNIQUE")
        print("Constraints created successfully.")
    
    
    def build_graph(self):
        tools_data = self._load_data("data/tools.json")
        workflows_data = self._load_data("data/workflows.json")
        
        print("--- Starting to build the graph ---")
        
        for tool in tqdm(tools_data, desc="Processing Tools"):
            text_to_embed = f"Tool: {tool['name']}\nTool_id: {tool['id']}\nDescription: {tool['description']}\nHelp: {tool['help_text']}"
            embedding = get_embedding(text_to_embed)
            
            if not embedding:
                print(f"Skipping tool {tool['id']} due to embedding failure.")
                continue
            
            tool_query = """
            MERGE (t:Tool {id: $id})
            SET t.name = $name,
                t.description = $description,
                t.embedding = $embedding
            """
            tool_params = {
                'id': tool['id'], 
                'name': tool['name'], 
                'description': tool['description'], 
                'embedding': embedding[0]
            }
            self.db.execute_query(tool_query,tool_params)
            
            if tool.get('category'):
                cat_query = """
                MERGE (c:Category {name: $cat_name})
                MERGE (t:Tool {id: $tool_id})
                MERGE (t)-[:BELONGS_TO]->(c)
                """
                self.db.execute_query(cat_query, {'cat_name': tool['category'], 'tool_id': tool['id']})
            
            for format_name in tool.get('input_formats', []):
                if format_name:
                    in_format_query = """
                    MERGE (f:FileFormat {name: $format_name})
                    MERGE (t:Tool {id: $tool_id})
                    MERGE (t)-[:ACCEPTS_INPUT]->(f)
                    """
                    self.db.execute_query(in_format_query, {'format_name': format_name, 'tool_id': tool['id']})

            for format_name in tool.get('output_formats', []):
                if format_name:
                    out_format_query = """
                    MERGE (f:FileFormat {name: $format_name})
                    MERGE (t:Tool {id: $tool_id})
                    MERGE (t)-[:PRODUCES_OUTPUT]->(f)
                    """
                    self.db.execute_query(out_format_query, {'format_name': format_name, 'tool_id': tool['id']})
            
        for workflow in tqdm(workflows_data, desc="Processing Workflows"):
            # Create Workflow Node
            wf_query = """
            MERGE (w:Workflow {id: $id})
            SET w.name = $name, w.num_steps = $num_steps
            """
            self.db.execute_query(wf_query, {
                'id': workflow['id'],
                'name': workflow['name'],
                'num_steps': workflow['num_steps']
            })

            # Create Relationships to Tools
            for tool_id in workflow.get('included_tools', []):
                rel_query = """
                MATCH (w:Workflow {id: $wf_id})
                MATCH (t:Tool {id: $tool_id})
                MERGE (w)-[:INCLUDES_TOOL]->(t)
                """
                self.db.execute_query(rel_query, {'wf_id': workflow['id'], 'tool_id': tool_id})

        print("--- Graph building process complete! ---")
            
        