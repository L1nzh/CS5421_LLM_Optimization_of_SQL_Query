from __future__ import annotations

import pytest

from layer3.models import DOUBAO_SEED_2_0_LITE_260215, GPT_5_4_NANO


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
