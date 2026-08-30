# Telegram Audio Editor Bot

A Python Telegram bot for bulk audio editing, metadata modification, trimming, merging, and audio processing.

## Features

- 🎵 Process MP3, M4A, and FLAC audio
- ✏️ Edit artist metadata
- 🔢 Set title / title prefix and counters
- 🖼️ Set, remove, or keep original cover
- ➕ Add voice tag
- ➖ Remove voice tag
- 📢 Set a target channel
- 📦 Batch process channel posts
- 🔗 Merge multiple audio files
- 📝 Enable or disable captions
- 🗑️ Configure start/end trimming
- 📊 Download and upload progress reporting
- 👤 User authorization and admin/subscription management
- 💾 SQLite settings and usage tracking
- ⚡ Async processing with configurable concurrency
- 🚀 Local Telegram Bot API support for local file handling

## Tech Stack

- Python 3.13+
- Pyrogram
- TgCrypto
- FFmpeg
- Mutagen
- PyDub
- aiohttp
- aiofiles
- aiosqlite
- Telegram Local Bot API

## Installation

```bash
git clone https://github.com/vsiva9649/telegram-audio-editor-bot.git
cd telegram-audio-editor-bot

python3 -m venv .venv
source .venv/bin/activate

python -m pip install -r requirements.txt
```

Install FFmpeg on Debian/Kali/Ubuntu:

```bash
sudo apt update
sudo apt install -y ffmpeg
```

Verify:

```bash
python3 --version
ffmpeg -version
```

## Configuration

Configure these values before running:

```python
API_ID = ...
API_HASH = "..."
BOT_TOKEN = "..."
SUPER_ADMIN_ID = ...
```

**Never commit real Telegram credentials to a public repository.**

For production, environment variables are recommended:

```python
import os

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SUPER_ADMIN_ID = int(os.environ["SUPER_ADMIN_ID"])
```

## Run

```bash
source .venv/bin/activate
python audioeditor.py
```

## systemd

Example `/etc/systemd/system/audioeditor.service`:

```ini
[Unit]
Description=Audio Editor Telegram Bot
After=network.target telegram-bot-api.service

[Service]
User=kali
WorkingDirectory=/home/kali/pythonprojects/PythonTelegramBot
ExecStart=/home/kali/pythonprojects/PythonTelegramBot/.venv/bin/python /home/kali/pythonprojects/PythonTelegramBot/audioeditor.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable audioeditor
sudo systemctl start audioeditor
```

Useful commands:

```bash
sudo systemctl status audioeditor --no-pager
sudo systemctl restart audioeditor
sudo systemctl stop audioeditor
sudo journalctl -u audioeditor -f
```

## Local Telegram Bot API

The bot supports Telegram's Local Bot API for local file processing.

Example local endpoint:

```text
http://127.0.0.1:8081
```

Check the service:

```bash
sudo systemctl status telegram-bot-api --no-pager
```

## Runtime Files

The following are generated during runtime and should not be committed:

```text
bot_data.db
bot.log
downloads/
.venv/
```

## Security

Keep API credentials and bot tokens out of GitHub. If a bot token is ever exposed, revoke/rotate it and update the production configuration.

## License

Add your preferred open-source license before publishing.
