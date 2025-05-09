# Decimal Stripper Telegram Bot

A Telegram bot that processes messages containing numbers, separating them into amounts and charges based on a threshold value.

## Features

- Automatically categorizes numbers into amounts (>50) and charges (≤50)
- Strips decimal parts from amounts while keeping charges intact
- Supports various number formats (whole numbers, decimals, currency symbols)
- Customizable settings for decimal separator and output format
- Export results as CSV or JSON files

## Setup

1. Clone this repository or download the files
2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Create a `.env` file based on the `.env.example` template:
   ```
   cp .env.example .env
   ```
4. Edit the `.env` file and add your Telegram Bot Token (obtained from [@BotFather](https://t.me/botfather))

## Running the Bot

```
python main.py
```

The bot will start in polling mode and listen for messages.

## Bot Commands

- `/start` - Begin collecting messages
- `/help` - Show help message
- `/process` - Process all collected messages and separate amounts and charges
- `/clear` - Start over with a new collection
- `/settings` - Customize your number processing preferences
- `/stats` - View statistics about your collected messages
- `/export_csv` - Export results as a CSV file
- `/export_json` - Export results as a JSON file

## How It Works

- Values > 50 are considered 'Amounts' and decimal parts are stripped
- Values ≤ 50 are considered 'Charges' and kept as they are
- Forward messages containing numbers to the bot
- Use `/process` when you're done collecting messages

## Supported Number Formats

- Whole numbers (123)
- Standard decimal (123.45)
- Comma separator (123,45)
- With currency symbols ($123.45, €123,45)
- Negative values (-123.45)