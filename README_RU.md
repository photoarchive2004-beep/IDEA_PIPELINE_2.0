# IDEA_PIPELINE 2.0 — Stage B1 (очень кратко)

1. Скопируйте `config\secrets.env.example` в `config\secrets.env` и заполните `OPENALEX_API_KEY`.
2. Создайте идею через `1_NEW_IDEA.bat` (или подготовьте `ideas/<IDEA>/in/idea.txt`).
3. Запустите `RUN_B.bat` и выберите режим (FOCUSED/BALANCED/WIDE).
4. Если Stage B1 попросит LLM-шаг: prompt уже в `out/llm_prompt_B1_anchors.txt` (и в буфере), вставьте его в ChatGPT, получите **только JSON**, вставьте JSON в `in/llm_response_B1_anchors.json`, снова запустите `RUN_B.bat`.
5. Проверьте артефакты в `ideas/<IDEA>/out`: `corpus.csv`, `corpus_all.csv`, `search_log.json`, `prisma_lite.md`, `stageB1_summary.txt`, `checkpoint.json`.


В `stageB1_summary.txt` теперь всегда есть строки `STATUS = OK|DEGRADED|WAITING_FOR_LLM`, `STOP_REASON`, `PROMPT_FILE`, `WAIT_FILE`.

## Что нового в Stage B1 (vNext)
- Архивация `out` на каждом запуске: старые файлы перемещаются в `out/_archive/<timestamp>/`.
- LLM-лимит мигрирован на `10` (из старых `3`).
- Лимит LLM можно переопределить через `STAGE_B1_LLM_LIMIT` (по умолчанию `10`, с миграцией старых `limit<10`).
- Launcher читает `STATUS`, `STOP_REASON`, `PROMPT_FILE`, `WAIT_FILE` из `stageB1_summary.txt` и не использует хардкод-пути.

## SECURITY NOTE
Если в старых логах/артефактах Stage B1 ранее встречался `OPENALEX_API_KEY`, ключ нужно немедленно перевыпустить в OpenAlex и обновить `config/secrets.env`.

## Проверка после запуска (3 шага)
1. Откройте `ideas/<IDEA>/out/stageB1_summary.txt` и проверьте `STATUS = OK` или `STATUS = DEGRADED`.
2. Убедитесь, что в `ideas/<IDEA>/out` есть: `corpus_all.csv`, `corpus.csv`, `search_log.json`, `prisma_lite.md`, `stageB1_summary.txt`.
3. Если нужен LLM-шаг, используйте только `PROMPT_FILE` и `WAIT_FILE` из summary: вставить prompt в ChatGPT и сохранить JSON в wait-файл, без ручных правок других JSON.

## Проверка RUN_B: анти-«тихий вылет»

Проверены и задокументированы 3 сценария:
1. **Ошибка пути IDEA**: `RUN_B.bat` с несуществующим `IdeaDir` должен показать `FAILED code ...`, открыть `launcher_logs\\runB_last.log`, сделать `PAUSE`.
2. **Ошибка Python (exit code != 0)**: при неуспешном завершении Python `RUN_B.bat` также должен показать `FAILED code ...`, открыть `launcher_logs\\runB_last.log`, сделать `PAUSE`.
3. **Успех**: при успешном прогоне `RUN_B.bat` выводит `OK` и завершает работу с кодом `0`.

Технические гарантии в коде:
- `tools/run_b_launcher.ps1` всегда создаёт `launcher_logs/runB_last.log` со строкой `START run_b_launcher ...` и в `catch` дублирует ошибку в `runB_launcher_error.log` и `runB_last.log`.
- `tools/run_b.ps1` всегда пишет `START run_b.ps1 ...`, логирует исключения и возвращает `exit 1` на ошибках.
- `RUN_B.bat` при ошибке открывает `runB_last.log` (и только если его нет — `runB_launcher_error.log`), печатает путь логов и ждёт `PAUSE`.
