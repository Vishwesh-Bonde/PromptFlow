"""Deterministic instruction classification tests."""

from __future__ import annotations

import pytest

from app.core.instruction_classifier import classify_instruction
from app.core.state import InstructionKind


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("Start a new task about databases.", InstructionKind.NEW_TASK),
        ("Actually, use supervised learning instead.", InstructionKind.CORRECTION),
        ("Do not mention neural networks.", InstructionKind.RESTRICTION),
        ("Focus on practical examples.", InstructionKind.REFOCUS),
        ("Explain it using simple English.", InstructionKind.MODIFY),
        ("Add college-level examples.", InstructionKind.ADDITION),
        ("Please make sure the answer is accurate.", InstructionKind.ADDITION),
    ],
)
def test_instruction_classification_is_deterministic(
    content: str, expected: InstructionKind
) -> None:
    assert classify_instruction(content) is expected
    assert classify_instruction(content) is expected


def test_empty_instruction_has_a_safe_default() -> None:
    assert classify_instruction("   ") is InstructionKind.ADDITION
