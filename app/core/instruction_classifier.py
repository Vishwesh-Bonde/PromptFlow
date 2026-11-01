"""Deterministic instruction classification.

Classification is deliberately metadata only. The original instruction text is
always persisted and passed to the model unchanged; a heuristic can never cause
user content to be dropped or rewritten.
"""

from __future__ import annotations

import re

from app.core.state import InstructionKind

_PATTERNS: tuple[tuple[InstructionKind, re.Pattern[str]], ...] = (
    (
        InstructionKind.NEW_TASK,
        re.compile(
            r"\b(?:new|separate|different|another)\s+task\b"
            r"|\bstart\s+(?:over|fresh|again)\b"
            r"|\bcreate\s+(?:a\s+)?(?:new|separate)\s+task\b"
        ),
    ),
    (
        InstructionKind.CORRECTION,
        re.compile(
            r"\b(?:actually|correction|correct\s+(?:that|this)|i\s+meant|"
            r"mistake|wrong|instead)\b"
            r"|\bnot\s+[^.!?]{1,80}\s+but\b"
            r"|\b(?:change|replace)\s+[^.!?]{1,80}\s+(?:to|with)\b"
        ),
    ),
    (
        InstructionKind.RESTRICTION,
        re.compile(
            r"\b(?:only|avoid|without|except|exclude|limit|concise|brief|"
            r"short|fewer|at\s+most|no\s+more\s+than|don't|do\s+not)\b"
        ),
    ),
    (
        InstructionKind.REFOCUS,
        re.compile(
            r"\b(?:focus|refocus|emphasi[sz]e|prioriti[sz]e|concentrate|"
            r"shift|more\s+about|less\s+about|talk\s+about)\b"
        ),
    ),
    (
        InstructionKind.MODIFY,
        re.compile(
            r"\b(?:change|modify|rewrite|revise|update|adjust|simplify|"
            r"shorten|expand|rephrase|format|make\s+it|turn\s+this|use|using)\b"
        ),
    ),
    (
        InstructionKind.ADDITION,
        re.compile(
            r"\b(?:add|include|also|additionally|append|cover|mention|"
            r"give|provide|example|examples)\b"
        ),
    ),
)


def classify_instruction(content: str) -> InstructionKind:
    """Classify an instruction without calling a model or altering its text."""
    normalized = content.casefold()
    for kind, pattern in _PATTERNS:
        if pattern.search(normalized):
            return kind
    return InstructionKind.ADDITION
