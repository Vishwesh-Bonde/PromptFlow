"""Request schema and size-limit tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.schemas.task import (
    InstructionCreateRequest,
    TaskCreateRequest,
    enforce_max_chars,
)


def test_prompt_is_preserved_verbatim() -> None:
    request = TaskCreateRequest(prompt="  Explain ML.\n")
    assert request.prompt == "  Explain ML.\n"


def test_blank_prompt_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TaskCreateRequest(prompt="   \n\t")


def test_empty_instruction_is_rejected() -> None:
    with pytest.raises(ValidationError):
        InstructionCreateRequest(content="")


def test_blank_instruction_is_rejected() -> None:
    with pytest.raises(ValidationError):
        InstructionCreateRequest(content=" \n ")


def test_blank_client_instruction_id_is_rejected() -> None:
    with pytest.raises(ValidationError):
        InstructionCreateRequest(content="Use simpler words", client_instruction_id=" ")


def test_extra_request_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        TaskCreateRequest(prompt="hello", unexpected=True)  # type: ignore[call-arg]


def test_enforce_max_chars_accepts_boundary_value() -> None:
    assert enforce_max_chars("abc", 3, "prompt") == "abc"


def test_enforce_max_chars_rejects_oversized_value_without_echoing_it() -> None:
    text = "private user text"
    with pytest.raises(ValueError) as exc_info:
        enforce_max_chars(text, 5, "prompt")
    assert "private user text" not in str(exc_info.value)
    assert "5" in str(exc_info.value)
    assert str(len(text)) in str(exc_info.value)
