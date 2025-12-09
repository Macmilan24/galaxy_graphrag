import google.generativeai as genai
from src.graph_db.neo4j_manager import Neo4jManager
from config import settings
from src.utils.logger import get_logger
import time

logger = get_logger("community_summarizer")

class CommunitySummarizer:
    """
    Generates summaries for the Hierarchical Community Structure.
    1. Summarizes Level 1 Communities (Topics) based on all contained Tools/Workflows.
    2. Summarizes Level 2 SubCommunities based on their specific members.
    """
    def __init__(self):
        self.neo4j = Neo4jManager()
        if not settings.GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY is not set.")
        genai.configure(api_key=settings.GEMINI_API_KEY)
        # Using flash for speed and to avoid rate limits
        self.model = genai.GenerativeModel('gemini-2.5-flash')

    def fetch_communities(self):
        """Fetch Level 1 Communities and their members."""
        logger.info("Fetching Level 1 Communities...")
        query = """
        MATCH (c:Community)
        OPTIONAL MATCH (n)-[:IN_COMMUNITY]->(c)
        WITH c, n
        WHERE n:Tool OR n:Workflow
        RETURN c.id AS id, collect(labels(n)[0] + ": " + n.name + " - " + coalesce(n.description, '')) AS members
        """
        return self.neo4j.execute_query(query)

    def fetch_subcommunities(self):
        """Fetch Level 2 SubCommunities and their members."""
        logger.info("Fetching Level 2 SubCommunities...")
        query = """
        MATCH (s:SubCommunity)
        OPTIONAL MATCH (n)-[:IN_SUBCOMMUNITY]->(s)
        WITH s, n
        WHERE n:Tool OR n:Workflow
        RETURN s.id AS id, collect(labels(n)[0] + ": " + n.name + " - " + coalesce(n.description, '')) AS members
        """
        return self.neo4j.execute_query(query)

    def generate_summary(self, members_list, level="Community"):
        if not members_list:
            return "Empty Community", "No members found."

        # Limit context
        context_text = "\n".join(members_list[:50]) 
        
        prompt = f"""
        You are an expert bioinformatician analyzing a Galaxy workflow graph.
        
        Level: {level}
        Members:
        {context_text}
        
        1. Analyze the common functionality.
        2. Provide a short, descriptive Title (e.g., "RNA-Seq Alignment").
        3. Provide a concise Summary (2-3 sentences).
        
        Format:
        Title: <Title>
        Summary: <Summary>
        """
        
        try:
            response = self.model.generate_content(prompt)
            text = response.text.strip()
            
            title = "Unknown"
            summary = "No summary generated."
            
            for line in text.split('\n'):
                if line.startswith("Title:"):
                    title = line.replace("Title:", "").strip()
                elif line.startswith("Summary:"):
                    summary = line.replace("Summary:", "").strip()
            
            return title, summary
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            time.sleep(2) # Backoff
            return "Error", "Could not generate summary."

    def run_summarization(self):
        # 1. Summarize Level 1 Communities
        communities = self.fetch_communities()
        logger.info(f"Summarizing {len(communities)} Level 1 Communities...")
        
        comm_updates = []
        for rec in communities:
            cid = rec['id']
            members = rec['members']
            logger.info(f"Summarizing Community {cid} ({len(members)} members)...")
            
            title, summary = self.generate_summary(members, level="High-Level Topic")
            logger.info(f" -> {title}")
            
            comm_updates.append({"id": cid, "title": title, "summary": summary})
            time.sleep(1)

        if comm_updates:
            query = """
            UNWIND $batch AS row
            MATCH (c:Community {id: row.id})
            SET c.name = row.title, c.summary = row.summary
            """
            self.neo4j.execute_batch(query, comm_updates)

        # 2. Summarize Level 2 SubCommunities
        subcommunities = self.fetch_subcommunities()
        logger.info(f"Summarizing {len(subcommunities)} Level 2 SubCommunities...")
        
        sub_updates = []
        for rec in subcommunities:
            sid = rec['id']
            members = rec['members']
            logger.info(f"Summarizing SubCommunity {sid} ({len(members)} members)...")
            
            title, summary = self.generate_summary(members, level="Functional Sub-Group")
            logger.info(f" -> {title}")
            
            sub_updates.append({"id": sid, "title": title, "summary": summary})
            time.sleep(1)

        if sub_updates:
            query = """
            UNWIND $batch AS row
            MATCH (s:SubCommunity {id: row.id})
            SET s.name = row.title, s.summary = row.summary
            """
            self.neo4j.execute_batch(query, sub_updates)
            
        logger.info("Hierarchical Summarization complete.")

    def close(self):
        self.neo4j.close()

if __name__ == "__main__":
    summarizer = CommunitySummarizer()
    try:
        summarizer.run_summarization()
    finally:
        summarizer.close()
