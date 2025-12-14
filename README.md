### Setup
1) python -m venv .venv
2) source .venv/bin/activate  (Windows: .venv\Scripts\activate)
3) pip install -r requirements.txt
4) Put credentials.json рядом с проектом (НЕ коммитьте)
5) Copy .env.example -> .env and fill:
   - SOURCE_FOLDER_ID
   - TARGET_FOLDER_ID
   - TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS

### Run
python -m src.main