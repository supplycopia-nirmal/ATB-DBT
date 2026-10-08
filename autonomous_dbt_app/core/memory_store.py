import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import chromadb
from chromadb.config import Settings
from autonomous_dbt_app.config import CHROMA_PERSIST_DIR

class MemoryStore:
    """
    ChromaDB-backed Vector Data Store for SupplyCopia DBT standards,
    cross-EHR/ERP schema signatures, self-healing error resolutions, and user feedback history.
    """
    def __init__(self, persist_directory: Optional[Path] = None):
        self.persist_dir = persist_directory or CHROMA_PERSIST_DIR
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        
        self.client = chromadb.PersistentClient(
            path=str(self.persist_dir),
            settings=Settings(allow_reset=True, anonymized_telemetry=False)
        )
        
        # Initialize collections
        self.standards_col = self.client.get_or_create_collection(
            name="supplycopia_standards",
            metadata={"description": "SupplyCopia DBT design guidelines, V4 lineage, macro rules"}
        )
        self.signatures_col = self.client.get_or_create_collection(
            name="ehr_erp_signatures",
            metadata={"description": "Heterogeneous EHR and ERP column signatures to canonical mappings"}
        )
        self.healing_col = self.client.get_or_create_collection(
            name="error_resolution_memory",
            metadata={"description": "Runtime DuckDB / DBT error traces and validated code patches"}
        )
        self.feedback_col = self.client.get_or_create_collection(
            name="user_feedback_memory",
            metadata={"description": "User adjustments, custom Python rules, and lineage edits"}
        )

    def add_standard(self, doc_id: str, content: str, metadata: Dict[str, Any]):
        self.standards_col.upsert(
            ids=[doc_id],
            documents=[content],
            metadatas=[metadata]
        )

    def query_standards(self, query: str, n_results: int = 3) -> List[Dict[str, Any]]:
        results = self.standards_col.query(query_texts=[query], n_results=n_results)
        items = []
        if results and "documents" in results and results["documents"]:
            for doc, meta in zip(results["documents"][0], results["metadatas"][0]):
                items.append({"content": doc, "metadata": meta})
        return items

    def add_ehr_signature(self, signature_id: str, column_name: str, canonical_field: str, source_system: str, details: str):
        self.signatures_col.upsert(
            ids=[signature_id],
            documents=[f"Source column '{column_name}' from {source_system} maps to canonical '{canonical_field}'. {details}"],
            metadatas={"source_column": column_name, "canonical_field": canonical_field, "source_system": source_system}
        )

    def match_column_semantic(self, col_name: str, n_results: int = 2) -> List[Dict[str, Any]]:
        results = self.signatures_col.query(query_texts=[col_name], n_results=n_results)
        matches = []
        if results and "metadatas" in results and results["metadatas"]:
            for meta, dist in zip(results["metadatas"][0], results["distances"][0] if "distances" in results else [0]*len(results["metadatas"][0])):
                matches.append({"metadata": meta, "distance": dist})
        return matches

    def add_error_resolution(self, error_pattern: str, fix_diff: str, context: str):
        err_id = f"err_{hash(error_pattern) % 10000000}"
        self.healing_col.upsert(
            ids=[err_id],
            documents=[f"Error pattern: {error_pattern}\nContext: {context}"],
            metadatas={"fix_diff": fix_diff, "error_pattern": error_pattern}
        )

    def query_error_resolution(self, error_trace: str, n_results: int = 2) -> List[Dict[str, Any]]:
        results = self.healing_col.query(query_texts=[error_trace], n_results=n_results)
        resolutions = []
        if results and "metadatas" in results and results["metadatas"]:
            for meta in results["metadatas"][0]:
                resolutions.append(meta)
        return resolutions

    def add_feedback(self, feedback_id: str, user_text: str, action_taken: str):
        self.feedback_col.upsert(
            ids=[feedback_id],
            documents=[user_text],
            metadatas={"action_taken": action_taken}
        )
