# HELP_RU: Stage B

## Как запустить
- Обычный запуск: `RUN_B.bat`
- Прямой запуск:
  - `powershell -ExecutionPolicy Bypass -File tools/run_b_launcher.ps1 -Scope balanced -N 300 -IdeaDir <PATH>`

## Ограничения запросов
Stage B использует budget guard + HTTP cache (`.cache/stage_b`) и checkpoint (`out/checkpoint.json`).
Повторный запуск на том же входе не делает лишних сетевых запросов.

## Что делать при DEGRADED
1. Проверьте `OPENALEX_API_KEY` в `config/secrets.env`.
2. Попробуйте режим `wide`.
3. Проверьте интернет/доступ к API.
4. Смотрите детали в `out/search_log.json`.
