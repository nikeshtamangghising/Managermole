# Decimal Stripper Telegram Bot

A Telegram bot that processes messages containing numbers, separating them into amounts and charges based on a threshold value. Now with enhanced banking features for Nepali banks.

## Features

- Automatically categorizes numbers into amounts (>50) and charges (≤50)
- Strips decimal parts from amounts while keeping charges intact
- Supports various number formats (whole numbers, decimals, currency symbols)
- Customizable settings for decimal separator and output format
- Export results as CSV or JSON files
- Banking features for Nepali banks:
  - Manually enter bank names and deposit amounts
  - Track deposits for specific banks
  - Calculate remaining limits for each bank

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
- `/settings` - Customize your number processing preferences and access banking features
- `/stats` - View statistics about your collected messages
- `/export_csv` - Export results as a CSV file with enhanced features:
  - Manually enter deposit amounts and bank names
  - Append to existing CSV files for daily tracking
  - Previous day's remaining balance is automatically used as today's starting balance
  - Automatically calculate running totals across multiple days
- `/export_json` - Export results as a JSON file

## Banking Features

- **Bank Deposit Entry**: 
  - Select from a list of Nepali banks
  - Manually enter deposit amounts
  - Track total deposits for each bank

- **Remaining Limit Calculator**:
  - Select a bank from the list
  - Enter the bank's deposit limit
  - Automatically calculate remaining limit (limit - total deposits)
  - Monitor your remaining deposit capacity for each bank

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