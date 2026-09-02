"""
Централизованная настройка логирования: консоль (stdout, забирает Docker) +
ротируемый файл рядом с БД. Файл переживает перезапуск контейнера, попадает
в бэкап тома и доступен из админки (`/admin`, вкладка «Логи»).
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

_FMT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")

# Заполняется в setup_logging(), чтобы админка знала фактический путь.
_active_file: Path | None = None


def _log_dir() -> Path:
    """Каталог для файла лога: LOG_DIR, либо каталог файла БД, либо ./data."""
    explicit = os.getenv("LOG_DIR", "").strip()
    if explicit:
        return Path(explicit)
    db_path = os.getenv("DB_PATH", "data/sentry.db").strip()
    if db_path and db_path != ":memory:":
        return Path(db_path).parent
    return Path("data")


def log_file_path() -> Path:
    return _active_file or (_log_dir() / os.getenv("LOG_FILE", "sentry.log"))


def _level_value(level: str | None) -> int:
    name = (level or os.getenv("LOG_LEVEL", "INFO")).strip().upper()
    return getattr(logging, name, logging.INFO)


def setup_logging(level: str | None = None) -> None:
    """Пересобирает обработчики корневого логгера. Идемпотентна."""
    global _active_file

    root = logging.getLogger()
    root.setLevel(_level_value(level))
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    fmt = logging.Formatter(_FMT)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    try:
        path = _log_dir() / os.getenv("LOG_FILE", "sentry.log")
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            path,
            maxBytes=int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024))),
            backupCount=int(os.getenv("LOG_BACKUP_COUNT", "5")),
            encoding="utf-8",
        )
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
        _active_file = path
    except OSError as exc:  # каталог недоступен на запись — остаёмся с консолью
        _active_file = None
        root.warning("не удалось открыть файл лога: %s", exc)

    # uvicorn ведёт свои логгеры — перенаправляем их в корневой
    for name in _UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            lg.removeHandler(h)
        lg.propagate = True

    # В режиме --reload watchfiles следит за всем каталогом проекта, включая
    # data/ (БД + файл лога). На каждую запись туда он пишет INFO «1 change
    # detected»; строка попадает в файл лога → новое изменение → новая строка —
    # получается бесконечный цикл. Перезапуск и так фильтруется по *.py, эти
    # сообщения не нужны — глушим до WARNING.
    logging.getLogger("watchfiles").setLevel(logging.WARNING)


def set_level(level: str) -> None:
    """Сменить уровень на лету (из админки)."""
    logging.getLogger().setLevel(_level_value(level))


def tail_log(lines: int = 200) -> list[str]:
    """Последние N строк файла лога. Ротация держит файл небольшим —
    читаем целиком и отдаём хвост."""
    path = log_file_path()
    if not path or not path.exists():
        return []
    lines = max(1, min(lines, 5000))
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            buf = f.readlines()
    except OSError as exc:
        return [f"<не удалось прочитать {path}: {exc}>"]
    return [ln.rstrip("\n") for ln in buf[-lines:]]
