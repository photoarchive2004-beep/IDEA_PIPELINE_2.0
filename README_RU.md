# IDEA_PIPELINE 2.0 — кратко (RU)

## Stage B1 (поиск литературы)
Stage B собирает корпус статей и всегда пишет артефакты в `ideas/<IDEA>/out`:
- `corpus.csv`
- `corpus_all.csv`
- `search_log.json`
- `prisma_lite.md`
- `stageB_summary.txt`
- `checkpoint.json`

Статусы:
- **OK** — целевые объёмы достигнуты.
- **DEGRADED** — часть источников/ключей недоступна, но артефакты созданы.
- **FAILED** — критическая ошибка до записи артефактов.

## Секреты
1. Скопируйте пример: `copy config\secrets.env.example config\secrets.env`
2. Заполните ключи локально.
3. `config/secrets.env` не коммитится (в `.gitignore`).

Для OpenAlex после 2026 обязателен `OPENALEX_API_KEY`. Без ключа Stage B работает в режиме DEGRADED.

## Ручной smoke test
1. Запустите `RUN_B.bat`.
2. Убедитесь, что в `ideas/<IDEA>/out` появились файлы выше.
3. Откройте `stageB_summary.txt` и проверьте причины DEGRADED/OK.

## Stage B: интерактивный шаг ANCHORS via ChatGPT
Если Stage B остановился для уточнения anchors, он создаёт:
- `ideas/<IDEA>/out/llm_prompt_B_anchors.txt`
- `ideas/<IDEA>/in/llm_response_B_anchors.json`

Что делать:
1. Откройте `llm_prompt_B_anchors.txt` (он также копируется в буфер обмена из launcher).
2. Вставьте prompt в ChatGPT.
3. Получите ответ строго в формате JSON, без поясняющего текста.
4. Вставьте JSON в `ideas/<IDEA>/in/llm_response_B_anchors.json` и сохраните файл.
5. Снова запустите `RUN_B.bat` для продолжения Stage B.
