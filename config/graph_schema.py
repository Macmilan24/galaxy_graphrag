"""
Neo4j Graph Schema Definition for Galaxy GraphRAG
"""

GRAPH_SCHEMA = {
    "nodes": {
        "Tool": {
            "properties": {
                "id": "STRING",
                "name": "STRING", 
                "description": "STRING",
                "version": "STRING",
                "help": "STRING",
                "license": "STRING",
                "requirements": "LIST",
                "embedding": "LIST OF FLOAT",
                "created_date": "DATE",
                "source": "STRING"  # 'bioblend' or 'scraped'
            },
            "constraints": ["id"]
        },
        "Workflow": {
            "properties": {
                "id": "STRING",
                "name": "STRING",
                "description": "STRING", 
                "tags": "LIST",
                "steps_count": "INTEGER",
                "embedding": "LIST OF FLOAT",
                "source": "STRING"
            },
            "constraints": ["id"]
        },
        "WorkflowStep": {
            "properties": {
                "step_id": "STRING",  # workflow_id + step_number
                "step_number": "INTEGER",
                "workflow_id": "STRING",
                "tool_inputs": "MAP",  # Store step-specific inputs
                "annotation": "STRING"
            },
            "constraints": ["step_id"]
        },
        "Category": {
            "properties": {
                "name": "STRING",
                "description": "STRING",
                "level": "STRING"  # 'main', 'sub', 'tool_type'
            },
            "constraints": ["name"]
        },
        "Format": {
            "properties": {
                "name": "STRING",  # 'BED', 'GFF', 'FASTQ', etc.
                "description": "STRING"
            },
            "constraints": ["name"]
        },
        "Community": {
            "properties": {
                "community_id": "STRING",
                "name": "STRING",
                "description": "STRING",
                "level": "INTEGER",  # For hierarchical communities
                "size": "INTEGER"
            },
            "constraints": ["community_id"]
        }
    },
    "relationships": {
        "HAS_STEP": {
            "from": "Workflow",
            "to": "WorkflowStep",
            "properties": {}
        },
        "USES_TOOL": {
            "from": "WorkflowStep", 
            "to": "Tool",
            "properties": {
                "step_label": "STRING"
            }
        },
        "NEXT_STEP": {
            "from": "WorkflowStep",
            "to": "WorkflowStep", 
            "properties": {
                "order": "INTEGER"
            }
        },
        "BELONGS_TO_CATEGORY": {
            "from": ["Tool", "Workflow"],
            "to": "Category",
            "properties": {
                "confidence": "FLOAT"
            }
        },
        "CONSUMES_FORMAT": {
            "from": "Tool",
            "to": "Format", 
            "properties": {
                "optional": "BOOLEAN",
                "multiple": "BOOLEAN"
            }
        },
        "PRODUCES_FORMAT": {
            "from": "Tool",
            "to": "Format",
            "properties": {
                "multiple": "BOOLEAN"
            }
        },
        "SIMILAR_TO": {
            "from": "Tool", 
            "to": "Tool",
            "properties": {
                "similarity_score": "FLOAT",
                "type": "STRING"  
            }
        },
        "PART_OF_COMMUNITY": {
            "from": ["Tool", "Workflow"],
            "to": "Community",
            "properties": {
                "membership_strength": "FLOAT"
            }
        }
    }
}