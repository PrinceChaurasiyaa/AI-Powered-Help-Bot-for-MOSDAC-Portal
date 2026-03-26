"""
rag/prompt_builder.py
─────────────────────────────────────────────────────────────────────────────
Builds the system prompt and user message sent to Claude.

Design decisions:
  • System prompt establishes MOSDAC domain expert persona
  • Retrieved chunks are formatted as numbered <context> blocks
  • Conversation history is included for multi-turn coherence
  • Source citations are injected so Claude can tell users where answers
    come from (URL or document name)
─────────────────────────────────────────────────────────────────────────────
"""

from typing import List

from config import RAG_CONTEXT_MAX_CHARS


SYSTEM_PROMPT = """You are the MOSDAC Help Bot — an expert assistant for the \
Meteorological and Oceanographic Satellite Data Archival Centre (MOSDAC) \
portal at mosdac.gov.in, operated by ISRO's Space Applications Centre (SAC), Ahmedabad.

Your job is to help scientists, researchers, and general users understand:
- MOSDAC satellite missions (INSAT-3D/3DR/3DS/3A, KALPANA-1, MeghaTropiques, \
SARAL-AltiKa, OCEANSAT-2/3, SCATSAT-1)
- Satellite payloads and sensors (Imager, Sounder, SAPHIR, MADRAS, OCM, etc.)
- Data products and formats (Level 1/2/3, NetCDF, HDF5)
- Open Data products (Ocean, Atmosphere, Land categories)
- How to register, order data, use SFTP, and access the portal
- Documents, ATBDs, handbooks, and product specifications

Guidelines:
1. Answer ONLY from the provided context. If the context doesn't cover the \
question, say so honestly — do not hallucinate.
2. Be concise and factual. Use bullet points for lists.
3. Always cite your source at the end: e.g. (Source: mosdac.gov.in/insat-3d-payloads)
4. For technical specs (resolution, channels, swath), quote exact values from context.
5. If the user asks about login, registration, or data download, give step-by-step help.
6. Keep answers focused on MOSDAC. For questions outside MOSDAC scope, \
politely note that and redirect to the portal.
"""


def build_context_block(chunks: List[dict]) -> str:
    """
    Format retrieved chunks as a numbered context block for the prompt.

    Each chunk is formatted as:
      [1] Type: Mission | Source: https://...
          <text content>
    """
    if not chunks:
        return "No relevant context retrieved."

    lines = []
    for i, chunk in enumerate(chunks, 1):
        node_type  = chunk.get("node_type", "Unknown")
        label      = chunk.get("label", "")
        source_url = chunk.get("source_url", "")
        text       = chunk.get("text", "").strip()

        # Header line
        header = f"[{i}] {node_type}: {label}"
        if source_url:
            header += f" | Source: {source_url}"

        # Optional metadata enrichment
        extras = []
        if chunk.get("orbit_type"):
            extras.append(f"Orbit: {chunk['orbit_type']}")
        if chunk.get("section_type"):
            extras.append(f"Section: {chunk['section_type']}")
        if chunk.get("category"):
            extras.append(f"Category: {chunk['category']}")
        if chunk.get("doc_type"):
            extras.append(f"Doc type: {chunk['doc_type']}")
        if extras:
            header += f" | {', '.join(extras)}"

        lines.append(header)
        lines.append(text)
        lines.append("")   # blank separator

    return "\n".join(lines).strip()


def build_messages(
    query: str,
    context_chunks: List[dict],
    history: List[dict],
) -> List[dict]:
    """
    Build the messages list for the Claude API.

    Returns list of {"role": "user"|"assistant", "content": "..."}

    Structure:
      [history turns…]  ← previous conversation
      user: <context block> + question
    """
    messages = []

    # ── Include conversation history ──────────────────────────
    for turn in history:
        messages.append({
            "role":    turn["role"],
            "content": turn["content"],
        })

    # ── Build the current user message ────────────────────────
    context_text = build_context_block(context_chunks)

    user_content = (
        f"<context>\n{context_text}\n</context>\n\n"
        f"Question: {query}"
    )

    messages.append({
        "role":    "user",
        "content": user_content,
    })

    return messages


def build_no_context_message(query: str, history: List[dict]) -> List[dict]:
    """
    Fallback message when no relevant context was found.
    Tells Claude to acknowledge the gap rather than hallucinate.
    """
    messages = list(history)
    messages.append({
        "role": "user",
        "content": (
            f"<context>\nNo relevant information found in the MOSDAC knowledge base.\n</context>\n\n"
            f"Question: {query}\n\n"
            f"Please acknowledge that you don't have specific information about this "
            f"in the MOSDAC knowledge base and suggest where the user might find it "
            f"(e.g. mosdac.gov.in, the help page, or contacting MOSDAC admin)."
        ),
    })
    return messages


def format_sources(chunks: List[dict]) -> str:
    """
    Format a compact list of unique source URLs/labels for display.
    Used in the CLI to show "Sources:" after each answer.
    """
    seen   = set()
    sources = []
    for chunk in chunks:
        url   = chunk.get("source_url", "")
        label = chunk.get("label", "")
        key   = url or label
        if key and key not in seen:
            seen.add(key)
            if url:
                # Shorten to path only
                from urllib.parse import urlparse
                path = urlparse(url).path.rstrip("/")
                sources.append(f"• {path or url}")
            else:
                sources.append(f"• {label}")
    return "\n".join(sources) if sources else ""
