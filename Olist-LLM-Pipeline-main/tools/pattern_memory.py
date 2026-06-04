import os
import chromadb
from loguru import logger

from config.settings import PipelineConfig

CHROMA_PATH = str(PipelineConfig.ROOT_DIR / "metadata" / "chroma_db")

class PatternMemory:
    def __init__(self):
        self.client = None
        self.collection = None
        self._init_chroma()
        
    def _init_chroma(self):
        try:
            os.makedirs("metadata", exist_ok=True)
            # Use persistent local storage so memory survives pipeline restarts
            self.client = chromadb.PersistentClient(path=CHROMA_PATH)
            self.collection = self.client.get_or_create_collection(name="incident_memory")
            logger.info("Memory | Local ChromaDB initialized successfully.")
        except Exception as e:
            logger.warning(f"Memory | ChromaDB initialization failed: {e}. Semantic caching will be bypassed.")
            
    def remember(self, error_msg: str, dataset: str, node: str, sql_fix: str):
        """Save a successful fix to pattern memory."""
        try:
            if self.collection:
                # Use a deterministic ID based on the error string hash
                doc_id = f"fix_{abs(hash(error_msg))}"
                self.collection.add(
                    documents=[error_msg],
                    metadatas=[{"dataset": dataset, "node": node, "sql_fix": sql_fix}],
                    ids=[doc_id]
                )
                logger.success(f"Memory | Incident saved to ChromaDB: ID={doc_id} | Fix={sql_fix[:80]}")
        except Exception as e:
            logger.error(f"Memory | Failed to save incident to ChromaDB: {e}")
            
    def recall(self, error_msg: str) -> str:
        """Query ChromaDB for similar past errors. Returns SQL/Pandas fix if semantic match is close."""
        try:
            if self.collection:
                # Query ChromaDB (returns top 1 match)
                results = self.collection.query(
                    query_texts=[error_msg],
                    n_results=1
                )
                if results and results["documents"] and results["distances"] and results["distances"][0]:
                    dist = results["distances"][0][0]
                    # ChromaDB distance: L2 distance. Distances < 0.35 are extremely high semantic matches.
                    if dist < 0.35:
                        metadata = results["metadatas"][0][0]
                        sql_fix = metadata.get("sql_fix")
                        matched_doc = results["documents"][0][0]
                        logger.success(
                            f"Memory | SEMANTIC MATCH FOUND! (distance={dist:.3f}). "
                            f"Recalled cached fix: '{sql_fix[:100]}'"
                        )
                        return sql_fix
        except Exception as e:
            logger.error(f"Memory | Failed to recall from ChromaDB: {e}")
        return None

pattern_memory = PatternMemory()
