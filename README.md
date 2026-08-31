# Sentry

Sentry ловит вебхуки о новых задачах из Яндекс Трекера и пишет в чат Яндекс
Мессенджера, когда за короткое окно (по умолчанию 30 минут) приходит несколько задач
**с одной категорией и одним тегом** — признак массовой проблемы.

Обращения заводит клиентская поддержка: оператор заполняет форму (сайт или
мобильное приложение), из неё создаётся задача в Трекере, дальше её разбирает
техническая поддержка. Sentry сигналит техподдержке о всплеске раньше, чем он
станет виден в очереди.

> Имя внутреннее и не связано с продуктом sentry.io. В Docker образ имеет
> локальный тег `sentry-fl`, чтобы не путать с публичным образом.

Теги `Сайт` и `МП` разделяют два канала обращений (сайт / мобильное приложение),
у которых пересекаются категории.

---

## Быстрый старт (Docker)

Нужен только **Docker** с плагином **compose** (Docker Desktop или `docker-ce` + `docker-compose-plugin`).

```bash
# 1. забрать проект на машину (git clone / scp / архив), затем:
cd sentry

# 2. создать конфиг из шаблона
cp .env.example .env

# 3. заполнить .env: YANDEX_MESSENGER_TOKEN, YANDEX_MESSENGER_CHAT_ID
#    и (рекомендуется) WEBHOOK_TOKEN — сгенерировать можно так:
python -c "import secrets; print(secrets.token_urlsafe(32))"

# 4. запустить
docker compose up -d --build
```

Готово. Проверка:

```bash
curl http://localhost:8000/health
# {"status":"healthy","storage":"sqlite",...}
```

Swagger UI: `http://<host>:8000/docs`

С `make` (на сервере) то же самое короче: `make up`, `make logs`, `make down`, `make token`.

---

## Настройка триггера в Яндекс Трекере

Нужен **один триггер** на создание задачи → действие **«Вызвать HTTP»**:

- **Метод:** `POST`
- **URL:** `http://<host>:8000/webhook`
- **Заголовки:** `Content-Type: application/json`
  и, если задан `WEBHOOK_TOKEN`: `X-Webhook-Token: <ваш токен>`
- **Тело:**

```json
{
  "issue_key": "{{issue.key}}",
  "summary": "{{issue.summary}}",
  "category": "{{issue.category}}",
  "tag": "{{issue.tags}}"
}
```

Триггер срабатывает после создания задачи и передаёт её поля как есть.
`category` и `tag` подставляются из реальных полей задачи (категория и «Теги»,
где стоит `Сайт` или `МП`) — ничего хардкодить не нужно. Прочие поля тела
сервис игнорирует.

---

## Обновление

```bash
git pull            # или скопировать новые файлы
docker compose up -d --build
```

Состояние хранится в SQLite-файле на томе `sentry-data` и переживает
пересборку/перезапуск контейнера. Полный сброс: `docker compose down -v` либо
`POST /api/v1/clear`.

---

## Переменные окружения

| Переменная | По умолч. | Назначение |
|---|---|---|
| `YANDEX_MESSENGER_URL` | `https://ymnb.av.ru/api/messages/send` | эндпоинт отправки сообщений |
| `YANDEX_MESSENGER_TOKEN` | — | OAuth 2.0 токен (`Authorization: Bearer <token>`) |
| `YANDEX_MESSENGER_CHAT_ID` | — | id чата для уведомлений (формат `0/0/<uuid>`) |
| `WEBHOOK_TOKEN` | пусто | если задан — `/webhook` и служебные ручки требуют заголовок `X-Webhook-Token` |
| `TRACKER_URL` | `https://tracker.yandex.ru` | база для ссылок на задачи |
| `DB_PATH` | `data/sentry.db` | путь к файлу SQLite (в Docker — `/app/data/...`) |
| `EVENTS_RETENTION_DAYS` | `365` | сколько дней хранить события аналитики (`0` — не чистить) |
| `WINDOW_MINUTES` | `30` | окно поиска дублей |
| `MAX_LOG_ENTRIES` | `200` | сколько последних записей хранить в каждом логе |
| `TIMEZONE_OFFSET` | `3` | сдвиг TZ для вывода времени |
| `PORT` | `8000` | порт на хосте |

---

## Эндпоинты

| Метод | Путь | Защищён `WEBHOOK_TOKEN` |
|---|---|---|
| `POST` | `/webhook` | да |
| `GET` | `/health` | нет |
| `GET` | `/api/v1/stats` | нет |
| `GET` | `/api/v1/log?limit=50` | да |
| `POST` | `/api/v1/clear` | да |
| `POST` | `/api/v1/test-notify` | да |
| `GET` | `/dashboard` | нет |
| `GET` | `/api/v1/analytics/overview?days=30` | нет |
| `GET` | `/api/v1/analytics/events?days=30` | да |
| `GET` | `/` , `/docs` | нет |

---

## Дашборд

`GET /dashboard` — самодостаточная HTML-страница (графики рисуются инлайн, без CDN,
работает во внутренней сети). Показывает: KPI-сводку, поток задач по времени,
топ категорий, каналы, всплески по времени, тепловую карту час×день недели и список
последних всплесков. Диапазон переключается (7 / 30 / 90 / 365 дней), есть автообновление.

Данные берутся из append-only таблицы `events` (одна строка на обработанную задачу),
хранятся `EVENTS_RETENTION_DAYS` дней. Для внешнего BI:

- `GET /api/v1/analytics/overview?days=N` — те же агрегаты, что на странице, одним ответом;
- `GET /api/v1/analytics/events?days=N&limit=&offset=` — сырые строки событий (под токеном).

`/dashboard` и `/api/v1/analytics/overview` открыты (как `/api/v1/stats`) — если хост
доступен извне, закройте его на сетевом уровне.

---

## Эксплуатация

- **Один процесс.** Состояние в SQLite-файле, доступ идёт через одно соединение
  под общим замком, поэтому масштабировать в несколько воркеров/реплик нельзя.
  `docker-compose.yml` рассчитан на один контейнер.
- **Бэкап.** Достаточно скопировать файл БД: `docker compose cp
  sentry:/app/data/sentry.db ./backup.db`.
- **Посмотреть содержимое БД:** `python db_inspect.py` локально или
  `docker compose exec sentry python db_inspect.py` в контейнере.
  Ещё быстрее — `GET /api/v1/log` и `GET /api/v1/stats`.
- **Аналитика:** `GET /dashboard` в браузере; счётчик событий — в `GET /api/v1/stats`
  (`events_total`).
- **Логи** контейнера: `docker compose logs -f`. Ротация настроена (3 файла по 10 МБ).
- **Автозапуск** после перезагрузки хоста: `restart: unless-stopped` уже задан.
- **Доступ извне.** Если сервер смотрит в интернет — обязательно задайте
  `WEBHOOK_TOKEN` и/или закройте порт файрволом, оставив доступ только Трекеру.
  Для локального доступа поменяйте проброс порта в compose на
  `"127.0.0.1:${PORT:-8000}:8000"`.

---

## Локальный запуск без Docker

```bash
python -m venv .venv
. .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py                   # поднимется на :8000 с автоперезагрузкой
```
