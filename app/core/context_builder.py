"""Build a provider request from durable task state.

The original prompt is kept as user content. Accepted instructions are supplied
in sequence order with their deterministic classifications. A previous draft is
reference material only; the provider is asked for one coherent replacement
answer, never an append operation.
"""

from __future__ import annotations

from app.database.records import GenerationRecord, InstructionRecord, TaskRecord
from app.services.ai_provider import GenerationRequest


def build_generation_request(
    *,
    task: TaskRecord,
    instructions: tuple[InstructionRecord, ...] = (),
    previous_generation: GenerationRecord | None = None,
) -> GenerationRequest:
    previous_draft = None
    previous_version = None
    if previous_generation is not None:
        previous_version = previous_generation.version
        if previous_generation.draft_text.strip():
            previous_draft = previous_generation.draft_text

    return GenerationRequest(
        prompt=task.original_prompt,
        instructions=tuple(instruction.content for instruction in instructions),
        instruction_kinds=tuple(
            instruction.kind.value for instruction in instructions
        ),
        previous_draft=previous_draft,
        previous_generation_version=previous_version,
    )
