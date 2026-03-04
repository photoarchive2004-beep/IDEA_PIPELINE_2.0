# IDEA_PIPELINE 2.0 — Stage B1 (очень кратко)

1. Скопируйте `config\secrets.env.example` в `config\secrets.env` и заполните `OPENALEX_API_KEY`.
2. Создайте идею через `1_NEW_IDEA.bat` (или подготовьте `ideas/<IDEA>/in/idea.txt`).
3. Запустите `RUN_B.bat` и выберите режим (FOCUSED/BALANCED/WIDE).
4. Если Stage B1 попросит LLM-шаг: prompt уже в `out/llm_prompt_B1_anchors.txt` (и в буфере), вставьте его в ChatGPT, получите **только JSON**, вставьте JSON в `in/llm_response_B1_anchors.json`, снова запустите `RUN_B.bat`.
5. Проверьте артефакты в `ideas/<IDEA>/out`: `corpus.csv`, `corpus_all.csv`, `search_log.json`, `prisma_lite.md`, `stageB1_summary.txt`, `checkpoint.json`.
