# Job Aggregator — План реализации

## Цель
Создать веб-приложение для агрегации вакансий с нескольких российских job-сайтов и Telegram-каналов по требованию (ручной запуск через браузер).

## Источники данных

| Источник | Способ сбора | Готовность |
|----------|-------------|------------|
| **careerspace.app** | Playwright (headless browser) | С нуля |
| **geekjob.ru** | Playwright (headless browser) | С нуля |
| **hh.ru** | MCP-сервер `theYahia/hh-mcp` в HTTP-режиме | Готовый |
| **superjob.ru** | MCP-сервер `theYahia/superjob-mcp` в HTTP-режиме | Готовый |
| **Telegram каналы** | Telethon (MTProto API) | Готовая библиотека |

## Архитектура

### Стек технологий
- **Backend:** FastAPI + Uvicorn (async по умолчанию)
- **Frontend:** HTML + Jinja2 + CSS (чистый HTML-шаблонизатор, без JS-фреймворков)
- **Парсинг:** Playwright (async_api), aiohttp (для MCP-клиентов)
- **Telegram:** Telethon
- **Конфигурация:** YAML
- **Хранение:** JSON + CSV (результаты поиска)

### Структура проекта

```text
job-aggregator/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI: роуты, обработка форм, оркестрация
│   ├── config.py            # Загрузка config.yaml, настройки MCP
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── base.py          # Абстрактный BaseParser (async search(query, limit) -> list[dict])
│   │   ├── careerspace.py   # Playwright-парсер careerspace.app
│   │   ├── geekjob.py       # Playwright-парсер geekjob.ru
│   │   ├── hh_mcp.py        # HTTP-клиент к hh-mcp (localhost:3001)
│   │   ├── superjob_mcp.py  # HTTP-клиент к superjob-mcp (localhost:3002)
│   │   └── telegram.py      # Telethon-клиент для Telegram-каналов
│   └── templates/
│       ├── base.html        # Общий layout (шапка, футер)
│       ├── index.html       # Форма поиска (запрос, чекбоксы источников, лимит)
│       └── results.html     # Таблица результатов + фильтры + summary
├── static/
│   └── style.css            # Стили для формы и таблицы
├── config.yaml              # Параметры поиска, список каналов, MCP-порты
├── requirements.txt         # Зависимости
└── README.md                # Инструкция по установке и запуску
```

## Поток работы (при поиске через веб)

1. **Пользователь открывает `http://localhost:8000/`**
   - Рендерится `index.html` с формой поиска.

2. **Заполняет форму и нажимает "Искать"**
   - Поле: текст запроса (например, "Python").
   - Чекбоксы: какие источники использовать (hh, superjob, careerspace, geekjob, telegram).
   - Поле: лимит результатов на источник.

3. **FastAPI обрабатывает POST /search**
   - Читает `config.yaml`.
   - Запускает MCP-серверы hh и SuperJob в фоне через `subprocess.Popen`:
     - `npx @theyahia/hh-mcp --http` (порт 3001)
     - `npx @theyahia/superjob-mcp --http` (порт 3002)
   - Делает health-check (`/health`) пока серверы не поднимутся.

4. **Параллельный сбор данных**
   - `asyncio.gather` запускает выбранные парсеры одновременно.
   - Каждый парсер возвращает `list[dict]`.

5. **Остановка MCP**
   - `process.terminate()` после завершения сбора.

6. **Нормализация**
   - Все записи приводятся к единой схеме:
     ```json
     {
       "title": "...",
       "company": "...",
       "salary": "...",
       "location": "...",
       "url": "...",
       "source": "hh|superjob|careerspace|geekjob|telegram",
       "published_at": "2024-06-02T10:00:00",
       "description": "...",
       "skills": ["..."]
     }
     ```

7. **Сохранение**
   - `output/vacancies_{timestamp}.json`
   - `output/vacancies_{timestamp}.csv`

8. **Рендеринг результатов**
   - `results.html` показывает таблицу, количество по источникам, время выполнения.

## Зависимости (requirements.txt)

```text
fastapi
uvicorn[standard]
jinja2
python-multipart
aiohttp
playwright
telethon
pyyaml
pandas
```

## Настройка Telegram (первый запуск)

1. Получить `api_id` и `api_hash` на [my.telegram.org](https://my.telegram.org).
2. Указать их в `config.yaml`.
3. При первом запуске `TelegramClient` запросит:
   - Номер телефона.
   - Код из Telegram.
   - Пароль 2FA (если включена).
4. После этого создается файл сессии (`session_name.session`) — дальше авторизация не требуется.

## Как добавить новый источник

1. Создать `parsers/newsite.py`, унаследовать от `BaseParser`.
2. Реализовать `async search(query, limit) -> list[dict]`.
3. Импортировать в `app/main.py`.
4. Добавить чекбокс в `templates/index.html`.
5. Добавить в `config.yaml`.

## Верификация после реализации

1. Установить зависимости: `pip install -r requirements.txt && playwright install`.
2. Запустить сервер: `uvicorn app.main:app --reload --port 8000`.
3. Открыть `http://localhost:8000`.
4. Ввести запрос, выбрать источники, нажать "Искать".
5. Проверить, что в `output/` появились JSON и CSV.
6. Убедиться, что данные из всех выбранных источников присутствуют и нормализованы.
7. Проверить, что MCP-процессы завершаются без зомби-процессов.

## Возможности расширения (фаза 2)

- `/api/search` — тот же поиск, но возвращает JSON (для интеграций).
- Экспорт в Excel прямо из таблицы.
- История поисков (сохранение в SQLite).
- Email-уведомления о новых вакансиях по сохраненному запросу.
- Фильтрация и сортировка в таблице (JavaScript).
