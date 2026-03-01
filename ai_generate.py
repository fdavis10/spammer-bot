from __future__ import annotations

import json
from typing import Optional


PROMPT_TEMPLATE = """Составь краткое объявление о вакансии для рассылки в Telegram. 
Текст должен быть живым, без канцелярита, до 200 слов.
Формат: plain text, можно с эмодзи для привлечения внимания.

Данные вакансии:
- Должность: {position}
- Описание / обязанности: {description}
- Требования: {requirements}
- Зарплата (если указана): {salary}
- Контакты / как откликнуться: {contacts}

Дополнительные пожелания: {extra}

Ответь только текстом объявления, без заголовков и лишнего."""


def _call_g4f(prompt: str, model: str = "gpt-4o-mini") -> Optional[str]:
    try:
        from g4f.client import Client
        client = Client()
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )
        if response and response.choices:
            return (response.choices[0].message.content or "").strip()
    except Exception as e:
        raise RuntimeError(f"g4f: {e}") from e
    return None


def _call_openrouter(prompt: str, api_key: str, model: str = "meta-llama/llama-3.2-3b-instruct:free") -> Optional[str]:
    try:
        import urllib.request
        import urllib.error

        body = json.dumps({
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 512,
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=body,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://spammer-bot.local",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode())
        choices = data.get("choices") or []
        if choices:
            content = choices[0].get("message", {}).get("content") or ""
            return content.strip()
    except urllib.error.HTTPError as e:
        msg = e.read().decode() if e.fp else str(e)
        raise RuntimeError(f"OpenRouter HTTP {e.code}: {msg}") from e
    except Exception as e:
        raise RuntimeError(f"OpenRouter: {e}") from e
    return None


def generate_vacancy_text(
    position: str,
    description: str = "",
    requirements: str = "",
    salary: str = "",
    contacts: str = "",
    extra: str = "",
    backend: str = "g4f",
    api_key: Optional[str] = None,
) -> str:
    prompt = PROMPT_TEMPLATE.format(
        position=position or "Не указано",
        description=description or "Не указано",
        requirements=requirements or "Не указано",
        salary=salary or "Не указано",
        contacts=contacts or "Написать в личку / в комментарий",
        extra=extra or "—",
    )
    if backend == "openrouter" and api_key:
        result = _call_openrouter(prompt, api_key)
    else:
        result = _call_g4f(prompt)
    return result or ""
