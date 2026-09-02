# Sentry — короткие команды. Требуется Docker с плагином compose.

.PHONY: up down restart rebuild logs ps token test run

## запустить (собрать при необходимости) в фоне
up:
	docker compose up -d --build

## остановить и удалить контейнер
down:
	docker compose down

## перезапустить
restart:
	docker compose restart

## пересобрать образ с нуля и поднять
rebuild:
	docker compose build --no-cache && docker compose up -d

## смотреть логи
logs:
	docker compose logs -f --tail=100

## статус
ps:
	docker compose ps

## сгенерировать значение для WEBHOOK_TOKEN
token:
	@python -c "import secrets; print(secrets.token_urlsafe(32))"

## прогнать автотесты
test:
	pytest

## локальный запуск без Docker (с автоперезагрузкой)
run:
	python -m app.main
