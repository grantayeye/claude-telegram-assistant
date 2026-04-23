"""
Vector memory system using ChromaDB with local embeddings.
Stores conversations, facts, and notes with semantic search.
"""

import json
import time
from datetime import datetime
from pathlib import Path

import chromadb
from chromadb.config import Settings


class Memory:
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = str(Path(__file__).parent / "memory_db")

        self.client = chromadb.PersistentClient(
            path=db_path,
            settings=Settings(anonymized_telemetry=False),
        )

        # Conversations — every user message + assistant response pair
        self.conversations = self.client.get_or_create_collection(
            name="conversations",
            metadata={"hnsw:space": "cosine"},
        )

        # Facts — extracted important info, saved explicitly or auto-detected
        self.facts = self.client.get_or_create_collection(
            name="facts",
            metadata={"hnsw:space": "cosine"},
        )

    def add_conversation(self, user_msg: str, assistant_msg: str,
                         session_id: str = None, timestamp: str = None):
        """Store a conversation exchange."""
        ts = timestamp or datetime.now().isoformat()
        doc_id = f"conv-{int(time.time() * 1000)}"

        # Store as combined text for better semantic matching
        combined = f"User: {user_msg}\nAssistant: {assistant_msg}"

        self.conversations.add(
            documents=[combined[:8000]],  # ChromaDB doc size limit
            metadatas=[{
                "user_msg": user_msg[:2000],
                "assistant_msg": assistant_msg[:2000],
                "session_id": session_id or "",
                "timestamp": ts,
                "type": "conversation",
            }],
            ids=[doc_id],
        )

    def add_fact(self, fact: str, source: str = "user", timestamp: str = None):
        """Store an important fact or note."""
        ts = timestamp or datetime.now().isoformat()
        doc_id = f"fact-{int(time.time() * 1000)}"

        self.facts.add(
            documents=[fact[:8000]],
            metadatas=[{
                "source": source,
                "timestamp": ts,
                "type": "fact",
            }],
            ids=[doc_id],
        )

    def add_job_result(self, job_name: str, result: str, timestamp: str = None):
        """Store a job execution result."""
        ts = timestamp or datetime.now().isoformat()
        doc_id = f"job-{int(time.time() * 1000)}"

        self.conversations.add(
            documents=[f"Job '{job_name}' result: {result}"[:8000]],
            metadatas=[{
                "job_name": job_name,
                "assistant_msg": result[:2000],
                "timestamp": ts,
                "type": "job_result",
            }],
            ids=[doc_id],
        )

    def search(self, query: str, n_results: int = 10,
               collection: str = "all") -> list:
        """Semantic search across memory. Returns list of results."""
        results = []

        if collection in ("all", "conversations"):
            try:
                conv_results = self.conversations.query(
                    query_texts=[query],
                    n_results=min(n_results, self.conversations.count() or 1),
                )
                for i, doc in enumerate(conv_results["documents"][0]):
                    meta = conv_results["metadatas"][0][i]
                    dist = conv_results["distances"][0][i] if conv_results.get("distances") else 0
                    results.append({
                        "text": doc,
                        "type": meta.get("type", "conversation"),
                        "timestamp": meta.get("timestamp", ""),
                        "distance": dist,
                        "meta": meta,
                    })
            except Exception:
                pass

        if collection in ("all", "facts"):
            try:
                fact_results = self.facts.query(
                    query_texts=[query],
                    n_results=min(n_results, self.facts.count() or 1),
                )
                for i, doc in enumerate(fact_results["documents"][0]):
                    meta = fact_results["metadatas"][0][i]
                    dist = fact_results["distances"][0][i] if fact_results.get("distances") else 0
                    results.append({
                        "text": doc,
                        "type": "fact",
                        "timestamp": meta.get("timestamp", ""),
                        "distance": dist,
                        "meta": meta,
                    })
            except Exception:
                pass

        # Sort by relevance (lower distance = more relevant)
        results.sort(key=lambda x: x["distance"])
        return results[:n_results]

    def get_context_for_prompt(self, query: str, max_chars: int = 2000) -> str:
        """Get relevant memory context to inject into a Claude prompt."""
        results = self.search(query, n_results=5)
        if not results:
            return ""

        context_parts = []
        total_chars = 0
        for r in results:
            # Only include reasonably relevant results (cosine distance < 0.5)
            if r["distance"] > 0.5:
                continue
            ts = r["timestamp"][:16].replace("T", " ") if r["timestamp"] else ""
            entry = f"[{ts}] {r['text'][:400]}"
            if total_chars + len(entry) > max_chars:
                break
            context_parts.append(entry)
            total_chars += len(entry)

        if not context_parts:
            return ""

        return "Relevant context from past conversations:\n" + "\n---\n".join(context_parts)

    def remember(self, text: str) -> str:
        """Explicitly save something to memory. Returns confirmation."""
        self.add_fact(text, source="explicit")
        return f"Remembered: {text[:100]}"

    def forget(self, query: str) -> str:
        """Find and delete facts matching a query."""
        results = self.facts.query(query_texts=[query], n_results=5)
        if not results["ids"][0]:
            return "Nothing found to forget."

        deleted = []
        for i, doc_id in enumerate(results["ids"][0]):
            dist = results["distances"][0][i] if results.get("distances") else 1
            if dist < 0.3:  # Only delete close matches
                self.facts.delete(ids=[doc_id])
                deleted.append(results["documents"][0][i][:80])

        if not deleted:
            return "No close matches found to forget."
        return f"Forgot {len(deleted)} item(s):\n" + "\n".join(f"- {d}" for d in deleted)

    def stats(self) -> dict:
        return {
            "conversations": self.conversations.count(),
            "facts": self.facts.count(),
        }
