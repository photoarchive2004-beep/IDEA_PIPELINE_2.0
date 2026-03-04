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
