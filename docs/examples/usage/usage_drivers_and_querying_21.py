# Test module converted from docs example - code-block 21
"""Minimal smoke test for drivers_and_querying example 21."""

from pydantic import BaseModel

__all__ = ("User", "test_example_21_pydantic_model")


class User(BaseModel):
    id: int
    name: str
    email: str


def test_example_21_pydantic_model() -> None:
    u = User(id=1, name="Alice", email="a@example.com")
    assert u.id == 1
