from __future__ import annotations

import os
from typing import Iterable, List

SYSTEM_PROMPT = (
    "You are a wire-service desk editor. Produce a single short brief. "
    "Output exactly one paragraph. No headings, no bullets, no quotation marks."
)

USER_TEMPLATE = (
    "These are the fifteen most frequent keywords from current world news feeds:\n"
    "{keywords}\n\n"
    "Write a single paragraph (maximum 80 words) summarising the news landscape. "
    "Name at least three concrete storylines implied by these keywords: a country, "
    "an institution, a conflict, a public figure, or an industry event. Stay "
    "specific and concrete; do not hedge."
)


def summarise(keywords: Iterable[str]) -> str:
    kws: List[str] = [k for k in (keywords or []) if k][:15]
    if not kws:
        return "Waiting for keywords - no feed batches yet."

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if api_key:
        try:
            return _call_anthropic(kws, api_key)
        except Exception as exc:
            print(f"[summary] Anthropic call failed, falling back: {exc!r}")
    return _offline_brief(kws)


def _call_anthropic(keywords: List[str], api_key: str) -> str:
    from anthropic import Anthropic

    client = Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": USER_TEMPLATE.format(keywords=", ".join(keywords)),
            }
        ],
    )
    parts = [
        block.text
        for block in response.content
        if getattr(block, "type", "") == "text"
    ]
    return " ".join(p.strip() for p in parts if p.strip())


def _offline_brief(keywords: List[str]) -> str:
    head = ", ".join(keywords[:8])
    return (
        f"Offline editorial digest. The current feed is concentrated around "
        f"{head}. The keyword cloud points to several co-occurring storylines; "
        f"consult the keyword table below for full frequencies. A live brief "
        f"will appear once ANTHROPIC_API_KEY is configured."
    )
