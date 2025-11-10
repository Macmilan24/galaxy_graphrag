# Galaxy GraphRAG

This project implements a Graph-based Retrieval-Augmented Generation (GraphRAG) system for analyzing scientific workflows from Galaxy. It extracts tool and workflow data from a Galaxy instance, builds a knowledge graph in Neo4j, and uses community detection algorithms to identify clusters of related tools. This structured graph can then be leveraged by Large Language Models (LLMs) to answer complex queries about workflow composition and tool relationships.

## Features

*   **Galaxy Data Extraction**: Fetches workflow and tool data from a Galaxy instance using its API. See [`src/data_extraction/galaxy_extractor.py`](src/data_extraction/galaxy_extractor.py).
*   **Neo4j Graph Construction**: Models workflows and tools as a graph, storing them in a Neo4j database. See [`src/graph_db/graph_builder.py`](src/graph_db/graph_builder.py).
*   **Community Detection**: Implements the Leiden algorithm to identify communities of tools that are frequently used together. See [`src/community_detection/leiden_algorithm.py`](src/community_detection/leiden_algorithm.py).
*   **LLM & Embedding Integration**: Configured to use Google Gemini for generative tasks and Hugging Face models for creating embeddings.

## Project Structure

```
.
├── .env.example
├── main.py
├── requirements.txt
├── config/
│   └── settings.py
├── data/
│   ├── tools.json
│   └── workflows.json
└── src/
    ├── community_detection/
    │   └── leiden_algorithm.py
    ├── data_extraction/
    │   └── galaxy_extractor.py
    ├── graph_db/
    │   ├── graph_builder.py
    │   └── neo4j_manager.py
    └── utils/
        └── embedding_utils.py
```

## Installation

1.  **Clone the repository:**
    ```sh
    git clone <your-repository-url>
    cd galaxy_graphrag
    ```

2.  **Install dependencies:**
    Make sure you have Python 3.8+ installed. Then, install the required packages from [requirements.txt](requirements.txt):
    ```sh
    pip install -r requirements.txt
    ```

3.  **Set up environment variables:**
    Copy the example environment file and fill in your credentials.
    ```sh
    cp .env.example .env
    ```
    Now, edit the `.env` file with your specific configuration.

## Configuration

The application requires the following environment variables to be set in the `.env` file, as defined in [.env.example](.env.example):

*   `GALAXY_URL`: The URL of your Galaxy instance.
*   `GALAXY_API_KEY`: Your API key for the Galaxy instance.
*   `NEO4J_URI`: The connection URI for your Neo4j database (e.g., `bolt://localhost:7687`).
*   `NEO4J_USER`: The username for your Neo4j database.
*   `NEO4J_PASSWORD`: The password for your Neo4j database.
*   `GEMINI_API_KEY`: Your API key for the Google Gemini LLM.
*   `HF_EMBEDDING_API_URL`: The endpoint URL for the Hugging Face embedding model.
*   `HF_API_TOKEN`: Your Hugging Face API token for accessing the embedding model.

## Usage

To run the main application pipeline, execute the [main.py](main.py) script:

```sh
python main.py
```

This will initiate the data extraction from Galaxy, build the graph in Neo4j, and perform any subsequent analysis steps defined in the main script.