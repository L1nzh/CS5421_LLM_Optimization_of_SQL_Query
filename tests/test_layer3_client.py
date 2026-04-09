from __future__ import annotations

import pytest

from layer3.models import DOUBAO_SEED_2_0_LITE_260215, GPT_5_4_NANO, MINIMAX_M2_5


class FakeOpenAI:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def test_get_model_client_uses_ark_for_doubao(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARK_API_KEY", "ark-test-key")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    import layer3.client as client_module

    monkeypatch.setattr(client_module, "_build_openai_client", lambda **kwargs: FakeOpenAI(**kwargs))

    client = client_module.get_model_client(DOUBAO_SEED_2_0_LITE_260215)

    assert client.kwargs["api_key"] == "ark-test-key"
    assert "ark.cn-beijing.volces.com" in client.kwargs["base_url"]


def test_get_model_client_uses_openai_for_gpt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "openai-test-key")
    monkeypatch.delenv("ARK_API_KEY", raising=False)

    import layer3.client as client_module

    monkeypatch.setattr(client_module, "_build_openai_client", lambda **kwargs: FakeOpenAI(**kwargs))

    client = client_module.get_model_client(GPT_5_4_NANO)

    assert client.kwargs["api_key"] == "openai-test-key"
    assert "base_url" not in client.kwargs


def test_get_model_client_requires_openai_key_for_gpt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    import layer3.client as client_module

    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        client_module.get_model_client(GPT_5_4_NANO)


def test_get_model_client_uses_minimax_for_minimax_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MINIMAX_API_KEY", "minimax-test-key")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ARK_API_KEY", raising=False)

    import layer3.client as client_module

    monkeypatch.setattr(client_module, "_build_openai_client", lambda **kwargs: FakeOpenAI(**kwargs))

    client = client_module.get_model_client(MINIMAX_M2_5)

    assert client.kwargs["api_key"] == "minimax-test-key"
    assert "api.minimax.io/v1" in client.kwargs["base_url"]


def test_get_model_client_requires_minimax_key_for_minimax_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MINIMAX_API_KEY", raising=False)

    import layer3.client as client_module

    with pytest.raises(RuntimeError, match="MINIMAX_API_KEY"):
        client_module.get_model_client(MINIMAX_M2_5)


def test_generate_text_uses_chat_completions_for_minimax(monkeypatch: pytest.MonkeyPatch) -> None:
    import layer3.candidate_generation as generation_module

    class FakeCompletions:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs):
            self.calls.append(kwargs)
            return type(
                "Response",
                (),
                {
                    "choices": [
                        type(
                            "Choice",
                            (),
                            {"message": type("Message", (), {"content": "SELECT 1"})()},
                        )()
                    ]
                },
            )()

    fake_completions = FakeCompletions()
    fake_client = type(
        "Client",
        (),
        {"chat": type("Chat", (), {"completions": fake_completions})()},
    )()
    monkeypatch.setattr(generation_module, "get_model_client", lambda model: fake_client)

    output = generation_module.generate_text("optimize me", MINIMAX_M2_5)

    assert output == "SELECT 1"
    assert fake_completions.calls == [
        {
            "model": MINIMAX_M2_5,
            "messages": [{"role": "user", "content": "optimize me"}],
        }
    ]
