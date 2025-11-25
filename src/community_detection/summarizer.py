import google.generativeai as genai
from src.graph_db.neo4j_manager import Neo4jManager
from config import settings
from src.utils.logger import get_logger
import time

logger = get_logger("community_summarizer")

class CommunitySummarizer:
    # Generates summaries for communities using Gemini and saves them to Neo4j.
    def __init__(self):
        self.neo4j = Neo4jManager()
        if not settings.GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY is not set.")
        genai.configure(api_key=settings.GEMINI_API_KEY)
        self.model = genai.GenerativeModel('gemini-2.5-pro')

    def fetch_communities(self):
        # Fetch tools grouped by community ID
        logger.info("Fetching communities from Neo4j...")
        query = """
        MATCH (t:Tool)
        WHERE t.communityId IS NOT NULL
        RETURN t.communityId AS community_id, collect(t.name + ": " + t.description) AS tools
        ORDER BY size(tools) DESC
        """
        return self.neo4j.execute_query(query)

    def generate_summary(self, tools_list):
        # Limit context to first 50 tools
        context_text = "\n".join(tools_list[:50]) 
        
        prompt = f"""
        You are an expert bioinformatician. 
        Here is a list of tools belonging to a specific community in a Galaxy workflow graph:
        
        {context_text}
        
        1. Analyze the common functionality of these tools.
        2. Provide a short, descriptive Title for this community (e.g., "RNA-Seq Analysis").
        3. Provide a concise Summary (2-3 sentences).
        
        Format your response exactly as:
        Title: <Title>
        Summary: <Summary>
        """
        
        try:
            response = self.model.generate_content(prompt)
            text = response.text.strip()
            
            title = "Unknown Community"
            summary = "No summary generated."
            
            for line in text.split('\n'):
                if line.startswith("Title:"):
                    title = line.replace("Title:", "").strip()
                elif line.startswith("Summary:"):
                    summary = line.replace("Summary:", "").strip()
            
            return title, summary
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return "Error", "Could not generate summary."

    def run_summarization(self):
        # Main loop: Fetch communities -> Summarize -> Update Neo4j
        communities = self.fetch_communities()
        logger.info(f"Found {len(communities)} communities to summarize.")
        
        updates = []
        
        for record in communities:
            comm_id = record['community_id']
            tools = record['tools']
            
            logger.info(f"Summarizing Community {comm_id} ({len(tools)} tools)...")
            title, summary = self.generate_summary(tools)
            logger.info(f" -> {title}")
            
            updates.append({
                "community_id": comm_id,
                "title": title,
                "summary": summary
            })
            time.sleep(1) # Rate limiting
            
        # Write back to Neo4j (Create Community nodes)
        logger.info("Writing summaries to Neo4j...")
        query = """
        UNWIND $batch AS row
        MERGE (c:Community {id: row.community_id})
        SET c.name = row.title,
            c.summary = row.summary
        
        WITH c, row
        MATCH (t:Tool {communityId: row.community_id})
        MERGE (t)-[:IN_COMMUNITY]->(c)
        """
        self.neo4j.execute_batch(query, updates)
        logger.info("Summarization complete.")

    def close(self):
        self.neo4j.close()

if __name__ == "__main__":
    summarizer = CommunitySummarizer()
    try:
        summarizer.run_summarization()
    finally:
        summarizer.close()
