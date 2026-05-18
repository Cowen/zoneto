"""Tests for the LLM client abstraction."""

from __future__ import annotations

from zoneto.api.llm_client import FakeLLMClient, LLMClient


class TestFakeLLMClient:
    def test_complete_returns_configured_response(self) -> None:
        """Given: FakeLLMClient with a fixed response.
        When: Calling complete().
        Then: Returns that exact string."""
        client = FakeLLMClient(response="Hello, world.")
        result = client.complete(system="sys", messages=[], max_tokens=100)
        assert result == "Hello, world."

    def test_stream_yields_full_response(self) -> None:
        """Given: FakeLLMClient with a fixed response.
        When: Consuming the stream.
        Then: Yields the configured response as a single chunk."""
        client = FakeLLMClient(response="Streamed text.")
        chunks = list(client.stream(system="sys", messages=[], max_tokens=100))
        assert chunks == ["Streamed text."]

    def test_default_response(self) -> None:
        """Given: FakeLLMClient with no explicit response.
        When: Calling complete().
        Then: Returns the default placeholder string."""
        client = FakeLLMClient()
        result = client.complete(system="sys", messages=[], max_tokens=100)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_satisfies_protocol(self) -> None:
        """Given: FakeLLMClient instance.
        When: Checking isinstance against LLMClient Protocol.
        Then: Passes runtime_checkable check."""
        client = FakeLLMClient()
        assert isinstance(client, LLMClient)

    def test_complete_ignores_messages(self) -> None:
        """Given: FakeLLMClient with a fixed response.
        When: Calling complete() with non-empty messages.
        Then: Always returns the configured response regardless of input."""
        client = FakeLLMClient(response="Always this.")
        messages = [
            {"role": "user", "content": "some question"},
            {"role": "assistant", "content": "some answer"},
        ]
        result = client.complete(system="sys", messages=messages, max_tokens=200)
        assert result == "Always this."
