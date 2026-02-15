# ENV для дневного competitors-парсера

## Как читается конфиг и приоритеты
- Загрузка: `src/config.py::load_settings()` через `load_dotenv('.env')`.
- Приоритет: переменные окружения процесса выше `.env` (поведение `python-dotenv` по умолчанию, без `override=True`).
- Типизация: большинство числовых значений читаются через `int(...)`/`float(...)`.
- Валидация:
  - `COMPETITORS_WINDOW_START/END` парсятся в `_parse_time_hhmm`; при ошибке используется дефолт.
  - Для `int/float` при невалидном значении будет исключение при старте.

## Переменные competitors

| Переменная | Тип | Дефолт | Допустимые значения | Где используется | Нюанс |
|---|---|---:|---|---|---|
| `COMPETITORS_ENABLED` | bool | `1` | `1/0`, `true/false`, `yes/no`, `on/off` | `src/config.py` | Флаг включения дневного режима (в `day_main`). |
| `COMPETITORS_WINDOW_START` | `HH:MM` | `09:00` | локальное время | `src/config.py`, `src/competitors_day_parser.py` | Начало окна работы. |
| `COMPETITORS_WINDOW_END` | `HH:MM` | `21:00` | локальное время | `src/config.py`, `src/competitors_day_parser.py` | Конец окна (правая граница не включается). |
| `COMPETITORS_TOTAL_ITEMS` | int | `300` | `>=1` | `src/config.py`, `src/competitors_day_parser.py` | Ограничивает входной список товаров сверху. |
| `COMPETITORS_MAX_ITEMS` | int | `=TOTAL_ITEMS` | `>=1` | `src/config.py` | Исторический alias (для day parser фактически не нужен). |
| `COMPETITORS_BATCH_SIZE` | int | `100` | `>=1` | `src/config.py`, `src/competitors_day_parser.py` | Размер «страницы» и фактическая квота в час в текущей реализации. |
| `COMPETITORS_ITEMS_PER_HOUR` | int | `100` | `>=1` | `src/config.py`, `src/competitors_day_parser.py` | Сейчас для совместимости: читается и логируется, но pacing считается от `BATCH_SIZE`. |
| `COMPETITORS_CYCLE_HOURS` | int | `3` | `>=1` | `src/config.py`, лог `src/competitors_day_parser.py` | Совместимость со старой схемой; не ограничивает цикл обработки. |
| `COMPETITORS_RUNS_PER_DAY` | int | `4` | `>=1` | `src/config.py` | Совместимость: в текущем daemon-режиме не ограничивает количество циклов. |
| `COMPETITORS_OFFERS_PER_PRODUCT` | int | `7` | `>=1` | `src/config.py`, `src/competitors_day_parser.py` | Сколько первых офферов продавцов анализировать по товару. |
| `COMPETITORS_SELLERS_LIMIT` | int | `=OFFERS_PER_PRODUCT` | `>=1` | `src/config.py` | Исторический alias. |
| `COMPETITORS_CITY` | string | `kiev` | slug города | `src/config.py`, `src/competitors_day_parser.py` | Используется для URL при скрейпе. |
| `COMPETITORS_OUTPUT_FILENAME` | string/path | `competitors_delivery_total.json` | путь к JSON | `src/config.py`, `src/competitors_day_parser.py` | Локальный файл результата + загрузка этого же имени в Drive. |
| `COMPETITORS_SAVE_EVERY_MINUTES` | int | `30` | `>=1` | `src/config.py`, `src/competitors_day_parser.py` | Периодический flush в локальный файл (atomic tmp+rename). |
| `COMPETITORS_STATE_FILE` | string/path | `out/competitors_state.json` | путь к JSON | `src/config.py`, `src/competitors_day_parser.py` | Состояние прогресса: день, offset, циклы, час. |
| `COMPETITORS_EXCLUDED_SELLERS` | list(string) | пусто | разделители `, ; |` | `src/config.py`, `src/competitors_day_parser.py` | Паттерны продавцов для исключения. |
| `COMPETITORS_EXCLUDED_CARD_IDS` | set(int) | пусто | числа, можно с мусором (`id=123`) | `src/config.py`, scraper | Чистится через regex первой числовой группы. |

## Браузерные переменные

| Переменная | Тип | Дефолт | Где используется | Нюанс |
|---|---|---:|---|---|
| `COMPETITORS_USE_BROWSER_FETCHER` | bool | `1` | `src/config.py`, `src/competitors_day_parser.py` | Разрешает fallback на Playwright. |
| `COMPETITORS_BROWSER_FIRST` | bool | `0` | `src/config.py`, scraper | Всегда идти сначала браузером. |
| `COMPETITORS_BROWSER_HEADLESS` | bool | `1` | `src/config.py`, `BrowserFetcher` | `0` — видимое окно браузера. |
| `COMPETITORS_BROWSER_TIMEOUT_SEC` | float | `45` | `src/config.py`, `BrowserFetcher` | Таймаут навигации в браузере. |
| `COMPETITORS_BROWSER_EXTRA_DELAY_SEC` | float | `2.0` | `src/config.py`, `BrowserFetcher` | Доп. ожидание после загрузки страницы. |
| `COMPETITORS_BROWSER_PROFILE_DIR` | string/path | `out/browser_profile` | `src/config.py`, `BrowserFetcher` | Профиль для cookie/session между запусками. |

## Turnstile/Cloudflare ожидание

| Переменная | Тип | Дефолт | Где используется | Нюанс |
|---|---|---:|---|---|
| `COMPETITORS_TURNSTILE_MANUAL_WAIT` | bool | `1` | `src/browser_fetcher.py` | При challenge ждёт ручное прохождение в окне браузера (не bypass). |
| `COMPETITORS_TURNSTILE_MAX_WAIT_SEC` | float | `600` | `src/browser_fetcher.py` | Максимальное ожидание ручной верификации. |

## Как считается прогресс сейчас
- Часовая порция: `items_per_hour = COMPETITORS_BATCH_SIZE`.
- Страницы: `offset` берётся из state-файла и двигается на `COMPETITORS_BATCH_SIZE`.
- Цикл: когда `offset` доходит до `COMPETITORS_TOTAL_ITEMS`, он сбрасывается в `0`, а `cycles_completed_today += 1`.
- День: счётчик `cycles_completed_today` ведётся для логов/диагностики, но обработка не останавливается по `RUNS_PER_DAY`.
- При смене даты `cycles_completed_today` сбрасывается, но `offset` сохраняется (если вчера окно закрылось посередине страницы, продолжение будет с этого места).
- Повторный старт в тот же час: если этот час уже закрыт (`last_hour_key`), запуск корректно завершится без дублей.
- Процесс работает в цикле постоянно: вне окна просто «спит» до следующего `COMPETITORS_WINDOW_START`, а не завершается.

## Пример для окна 07:00–21:00 (daemon-режим)
```env
COMPETITORS_WINDOW_START=07:00
COMPETITORS_WINDOW_END=21:00
COMPETITORS_TOTAL_ITEMS=300
COMPETITORS_BATCH_SIZE=50
COMPETITORS_ITEMS_PER_HOUR=50
COMPETITORS_RUNS_PER_DAY=4
COMPETITORS_SAVE_EVERY_MINUTES=30
COMPETITORS_STATE_FILE=out/competitors_state.json
COMPETITORS_OUTPUT_FILENAME=out/competitors_delivery_total.json
```

Ожидаемый порядок страниц при `TOTAL_ITEMS=300` и `BATCH_SIZE=50`: `0-50`, `50-100`, `100-150`, `150-200`, `200-250`, `250-300`, затем снова `0-50`.
