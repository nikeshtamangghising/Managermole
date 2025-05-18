
import re
import logging
import os
import time
import csv
import json
import socket
import threading
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, CallbackQueryHandler, Filters
from telegram.error import Conflict, TelegramError, NetworkError

# Import keep_alive function
from keep_alive import keep_alive

# Import for environment variables
try:
    from dotenv import load_dotenv
    # Load environment variables from .env file
    load_dotenv()
    logging.info("Loaded environment variables from .env file")
except ImportError:
    logging.warning("python-dotenv not installed. Using environment variables directly.")


# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Constants
AMOUNT_THRESHOLD = 50  # Values above this are considered amounts, otherwise charges

# Replace with your actual bot token - consider using an environment variable instead
BOT_TOKEN = os.environ.get('BOT_TOKEN')

# Dictionary to store collected messages for each user
user_messages = {}

# Dictionary to store user preferences
user_preferences = {}

# Default preferences
DEFAULT_PREFERENCES = {
    'decimal_separator': '.',  # Can be '.' or ','
    'include_currency': False,  # Whether to include currency symbols in output
    'output_format': 'simple',  # 'simple', 'detailed', or 'csv'
    'silent_collection': True   # Don't reply to every message during collection
}

# List of banks in Nepal
NEPAL_BANKS = [
    "Nepal Rastra Bank",
    "Agricultural Development Bank",
    "Nepal Bank Limited",
    "Rastriya Banijya Bank",
    "Nabil Bank",
    "Nepal Investment Bank",
    "Standard Chartered Bank Nepal",
    "Himalayan Bank",
    "Nepal SBI Bank",
    "Nepal Bangladesh Bank",
    "Everest Bank",
    "Bank of Kathmandu",
    "NCC Bank",
    "NIC Asia Bank",
    "Machhapuchhre Bank",
    "Kumari Bank",
    "Laxmi Bank",
    "Siddhartha Bank",
    "Global IME Bank",
    "Citizens Bank International",
    "Prime Commercial Bank",
    "Sunrise Bank",
    "Sanima Bank",
    "Mega Bank Nepal",
    "Civil Bank",
    "Century Commercial Bank",
    "Prabhu Bank",
    "Janata Bank Nepal",
    "Mahalaxmi Bikas Bank",
    "Garima Bikas Bank",
    "Muktinath Bikas Bank",
    "Jyoti Bikas Bank",
    "Excel Development Bank",
    "Shine Resunga Development Bank",
    "Tinau Development Bank",
    "Miteri Development Bank",
    "Green Development Bank",
    "Sindhu Bikas Bank",
    "Kamana Sewa Bikas Bank",
    "Gandaki Bikas Bank",
    "Lumbini Bikas Bank",
    "Corporate Development Bank",
    "Reliable Development Bank",
    "Infrastructure Development Bank",
    "Best Finance Company",
    "Pokhara Finance",
    "Goodwill Finance",
    "Reliance Finance",
    "Gurkhas Finance",
    "ICFC Finance",
    "Central Finance"
]

# Dictionary to store bank limits for each user
user_bank_limits = {}

# Dictionary to store bank deposits for each user
user_bank_deposits = {}

# Dictionary to store user-defined custom banks
user_custom_banks = {}

# Create a lock to ensure only one instance of the bot is running
BOT_INSTANCE_LOCK = threading.Lock()

# Function to handle errors
def error_handler(update, context):
    """Handle errors in the dispatcher."""
    try:
        if isinstance(context.error, Conflict):
            logging.warning("Conflict error: Another instance of the bot is already running")
            # Wait a bit and try to recover
            time.sleep(15)  # Increased wait time further
            try:
                # Try to restart polling to avoid conflicts
                if hasattr(context, 'bot') and hasattr(context.bot, 'get_updates'):
                    # More aggressive cleanup
                    context.bot.delete_webhook(drop_pending_updates=True)
                    logging.info("Deleted webhook and dropped pending updates")
                    
                    # Force a clean state
                    if hasattr(context.dispatcher, '_update_fetcher'):
                        if hasattr(context.dispatcher._update_fetcher, '_last_update_id'):
                            context.dispatcher._update_fetcher._last_update_id = 0
                            logging.info("Reset update ID to 0")
                    
                    # Add a small delay to ensure changes take effect
                    time.sleep(2)
                    logging.info("Completed conflict recovery process")
            except Exception as e:
                logging.error(f"Failed to recover from conflict: {e}")
                # Log the full traceback for better debugging
                import traceback
                logging.error(traceback.format_exc())
        elif isinstance(context.error, NetworkError):
            logging.error(f"Network error: {context.error}. Waiting before retry.")
            time.sleep(20)  # Increased wait time for network errors
        else:
            # Get update information safely
            update_str = str(update) if update else "None"
            logging.error(f"Update {update_str} caused error: {context.error}")
    except Exception as e:
        logging.error(f"Error in error handler: {e}")
        # Log the full traceback for better debugging
        import traceback
        logging.error(traceback.format_exc())

def start(update: Update, context) -> None:
    """Send a message when the command /start is issued."""
    user_id = update.effective_user.id
    username = update.effective_user.username or "there"

    # Initialize or reset the message collection for this user
    user_messages[user_id] = []

    # Initialize user preferences if not already set
    if user_id not in user_preferences:
        user_preferences[user_id] = DEFAULT_PREFERENCES.copy()

    update.message.reply_text(
        f"Hi {username}! I'm a Decimal Stripper Bot that can separate amounts and charges.\n\n"
        "Forward me messages containing numbers. I'll automatically categorize:\n"
        f"- Amounts (values > {AMOUNT_THRESHOLD}): decimal parts will be stripped\n"
        f"- Charges (values ≤ {AMOUNT_THRESHOLD}): kept exactly as they are\n\n"
        "When you're ready, use /process to see the separated results.\n\n"
        "Use /settings to customize how I process your numbers.\n"
        "Use /clear to start a new collection.\n"
        "Use /help for more information."
    )
def help_command(update: Update, context) -> None:
    """Send a message when the command /help is issued."""
    # This docstring should be indented to match the function definition
    update.message.reply_text(
        "Here's how to use this bot:\n\n"
        "📝 <b>Basic Commands</b>:\n"
        "/start - Begin collecting messages\n"
        "/help - Show this help message\n"
        "/process - Process all collected messages and separate amounts and charges\n"
        "/clear - Start over with a new collection\n"
        "/settings - Customize your number processing preferences and access banking features\n"
        "/stats - View statistics about your collected messages\n\n"

        "📊 <b>Export Options</b>:\n"
        "/export_csv - Export results in CSV format with two options:\n"
        "  - Simple: Just amounts, charges, and running sums row by row\n"
        "  - Detailed: Full format with Date, Deposit Amount, Bank Name, Paid To Host, Total Deposit, Total Paid, and Remaining Balance\n"
        "    • You can manually enter deposit amounts, bank names, and remaining balance\n"
        "    • You can append to existing CSV files for daily tracking\n"
        "    • Previous day's remaining balance is automatically used as today's starting balance\n"
        "    • Manually entered remaining balance takes precedence over previous day's balance\n"
        "    • Automatically calculates running totals across multiple days\n"
        "/export_json - Export results as a JSON file\n\n"

        "🏦 <b>Banking Features</b>:\n"
        "- Bank Deposit Entry: Manually enter deposits for specific Nepali banks\n"
        "- Remaining Limit Check: Calculate remaining limit by subtracting total deposits from bank limit\n"
        "- Access these features through the /settings menu\n\n"

        "💡 <b>How It Works</b>:\n"
        f"- Values > {AMOUNT_THRESHOLD} are considered 'Amounts' and decimal parts are stripped\n"
        f"- Values ≤ {AMOUNT_THRESHOLD} are considered 'Charges' and kept as they are\n"
        "- Use /process when you're done collecting messages\n\n"

        "🔎 <b>Supported Number Formats</b>:\n"
        "- Whole numbers (123)\n"
        "- Standard decimal (123.45)\n"
        "- Comma separator (123,45)\n"
        "- With currency symbols ($123.45, €123,45)\n"
        "- Negative values (-123.45)",
        parse_mode='HTML'
    )

def collect_message(update: Update, context) -> None:
    """Collect forwarded messages without replying to each one."""
    user_id = update.effective_user.id
    text = update.message.text

    if not text:
        # Only respond if the message has no text
        update.message.reply_text("Please forward me a text message.")
        return

    # Initialize message collection for this user if not already done
    if user_id not in user_messages:
        user_messages[user_id] = []

    # Initialize user preferences if not already set
    if user_id not in user_preferences:
        user_preferences[user_id] = DEFAULT_PREFERENCES.copy()

    # Add the message to the collection with metadata
    message_data = {
        'text': text,
        'timestamp': datetime.now().isoformat(),
        'message_id': update.message.message_id
    }
    user_messages[user_id].append(message_data)

    # Check if silent collection is enabled
    if not user_preferences[user_id]['silent_collection']:
        # Get the user's preferred decimal separator
        decimal_separator = user_preferences[user_id]['decimal_separator']

        # Create the appropriate pattern based on user preference
        if decimal_separator == '.':
            # Pattern for both whole numbers and decimals with period separator
            pattern = r'([€$£¥])?(\-?\d+(?:\.\d+)?)'
        else:
            # Pattern for both whole numbers and decimals with comma separator
            pattern = r'([€$£¥])?(\-?\d+(?:,\d+)?)'

        matches = re.findall(pattern, text)

        if matches:
            preview_numbers = []
            for match in matches:
                currency = match[0] if match[0] else ''
                number_str = match[1]

                # Get numeric value for classification
                if decimal_separator in number_str:
                    integer_part = number_str.split(decimal_separator)[0]
                    try:
                        value = float(integer_part)
                    except ValueError:
                        value = 0
                else:
                    try:
                        value = float(number_str)
                    except ValueError:
                        value = 0

                # For amounts (> AMOUNT_THRESHOLD), strip decimal part
                # For charges (≤ AMOUNT_THRESHOLD), keep as is
                if value > AMOUNT_THRESHOLD:
                    if decimal_separator in number_str:
                        processed_number = number_str.split(decimal_separator)[0]
                    else:
                        processed_number = number_str
                else:
                    # Keep charges as they are, with decimals
                    processed_number = number_str

                if user_preferences[user_id]['include_currency'] and currency:
                    preview_numbers.append(f"{currency}{processed_number}")
                else:
                    preview_numbers.append(processed_number)

            preview = ", ".join(preview_numbers)
            update.message.reply_text(
                f"✅ Message collected! Found these numbers: {preview}\n"
                f"📝 You now have {len(user_messages[user_id])} messages in your collection.\n"
                "Forward more messages or use /process when ready."
            )
        else:
            update.message.reply_text(
                f"✅ Message collected! (No numbers found)\n"
                f"📝 You now have {len(user_messages[user_id])} messages in your collection.\n"
                "Forward more messages or use /process when ready."
            )

def extract_number_value(match, decimal_separator, full_text):
    """Helper function to extract numeric value from a regex match.
    
    Args:
        match: Regex match object containing currency and number groups
        decimal_separator: The decimal separator character ('.' or ',')
        full_text: The complete text being processed

    Returns:
        tuple: (currency, number_str, processed_number, value, has_decimal)
    """
    currency = match[0] if match[0] else ''
    number_str = match[1]

    # Check if this is a decimal number
    has_decimal = decimal_separator in number_str

    # Get the integer part for classification
    if has_decimal:
        integer_part = number_str.split(decimal_separator)[0]
        try:
            value = float(integer_part)
        except ValueError:
            # Fallback if conversion fails
            value = 0
    else:
        # It's a whole number
        try:
            value = float(number_str)
        except ValueError:
            # Fallback if conversion fails
            value = 0

    # For amounts (> AMOUNT_THRESHOLD), strip decimal part
    # For charges (≤ AMOUNT_THRESHOLD), keep as is with decimal part
    if value > AMOUNT_THRESHOLD:
        if has_decimal:
            processed_number = integer_part
        else:
            processed_number = number_str
    else:
        # Keep charges as they are (with decimals if present)
        processed_number = number_str

    return currency, number_str, processed_number, value, has_decimal

def process_command(update: Update, context) -> None:
    """Process all collected messages and separate amounts (>50) and charges (≤50)."""
    user_id = update.effective_user.id

    if user_id not in user_messages or not user_messages[user_id]:
        update.message.reply_text("❗ No messages collected yet. Forward some messages first.")
        return

    # Get user preferences
    preferences = user_preferences.get(user_id, DEFAULT_PREFERENCES.copy())
    decimal_separator = preferences['decimal_separator']
    include_currency = preferences['include_currency']
    output_format = preferences['output_format']

    # Create the appropriate pattern based on user preference
    if decimal_separator == '.':
        # Pattern for both whole numbers and decimals with period separator
        pattern = r'([€$£¥])?(\-?\d+(?:\.\d+)?)'
    else:
        # Pattern for both whole numbers and decimals with comma separator
        pattern = r'([€$£¥])?(\-?\d+(?:,\d+)?)'

    amounts = []  # Values > AMOUNT_THRESHOLD
    charges = []  # Values ≤ AMOUNT_THRESHOLD
    extracted_data = []

    # Process all collected messages
    for message_data in user_messages[user_id]:
        message_text = message_data['text']

        # Find all matches in the message
        matches = re.findall(pattern, message_text)

        for match in matches:
            currency, original_number, processed_number, value, has_decimal = extract_number_value(match, decimal_separator, message_text)

            # Determine what to include in the result based on preferences
            extracted_value = ''
            if include_currency and currency:
                extracted_value = f"{currency}{processed_number}"
            else:
                extracted_value = processed_number

            # Add to appropriate category based on value
            if value > AMOUNT_THRESHOLD:
                amounts.append(extracted_value)
            else:
                charges.append(extracted_value)

            # Format match for display
            full_match = f"{currency}{original_number}"

            # Add to extracted data for export/detailed output
            extracted_data.append({
                'original_text': message_text,
                'full_match': full_match,
                'extracted_value': extracted_value,
                'currency': currency,
                'original_number': original_number,
                'processed_number': processed_number,
                'value': value,
                'has_decimal': has_decimal,
                'category': 'amount' if value > AMOUNT_THRESHOLD else 'charge',
                'message_id': message_data.get('message_id', 'unknown')
            })

    if extracted_data:
        # Format the output based on user preferences
        if output_format == 'simple':
            amounts_str = '\n'.join(amounts)
            charges_str = '\n'.join(charges)

            response = (
                f"📊 <b>Processed Results</b>\n\n"
                f"<b>Amounts (>{AMOUNT_THRESHOLD}):</b> [decimal parts stripped]\n{amounts_str if amounts else 'None found'}\n\n"
                f"<b>Charges (≤{AMOUNT_THRESHOLD}):</b> [kept exactly as found]\n{charges_str if charges else 'None found'}\n\n"
                f"Found {len(extracted_data)} numbers ({len(amounts)} amounts, {len(charges)} charges) from {len(user_messages[user_id])} messages.\n"
                "Use /export_csv or /export_json for detailed outputs."
            )

            update.message.reply_text(response, parse_mode='HTML')

        elif output_format == 'detailed':
            amounts_details = []
            charges_details = []

            for i, item in enumerate(extracted_data, 1):
                detail = f"{i}. Original: {item['full_match']} → Processed: {item['extracted_value']}"
                if item['category'] == 'amount':
                    amounts_details.append(detail)
                else:
                    charges_details.append(detail)

            amounts_text = "\n".join(amounts_details) if amounts_details else "None found"
            charges_text = "\n".join(charges_details) if charges_details else "None found"

            response = (
                f"📊 <b>Detailed Results</b>\n\n"
                f"<b>Amounts (>{AMOUNT_THRESHOLD}):</b> [decimal parts stripped]\n{amounts_text}\n\n"
                f"<b>Charges (≤{AMOUNT_THRESHOLD}):</b> [kept exactly as found]\n{charges_text}\n\n"
                f"Found {len(extracted_data)} numbers ({len(amounts)} amounts, {len(charges)} charges) from {len(user_messages[user_id])} messages."
            )

            # Check if response is too long
            if len(response) > 4000:  # Telegram message length limit
                response = (
                    f"📊 <b>Detailed Results (Truncated)</b>\n\n"
                    f"<b>Amounts Count:</b> {len(amounts)}\n"
                    f"<b>Charges Count:</b> {len(charges)}\n\n"
                    "The full detailed output is too long to display. Please use /export_csv or /export_json for the complete results."
                )

            update.message.reply_text(response, parse_mode='HTML')
    else:
        update.message.reply_text(
            f"❗ I couldn't find any numbers in your collected messages.\n"
            f"Try changing the decimal separator in /settings if your numbers use a different format."
        )

# Dictionary to store CSV file paths for each user
user_csv_files = {}

# Dictionary to store conversation states for each user
user_states = {}

def ask_for_deposit_info(update: Update, context) -> None:
    """Ask the user for deposit amount, bank name, and remaining balance."""
    user_id = update.effective_user.id
    
    # Initialize user state for CSV export
    user_states[user_id] = {
        'state': 'waiting_for_deposit_amount',
        'action': 'csv_export',
        'remaining_balance': None  # Will store the manually entered remaining balance
    }
    
    # Create a keyboard with Nepali banks and user's custom banks
    keyboard = []
    row = []
    
    # Add default Nepali banks
    for i, bank in enumerate(NEPAL_BANKS):
        if i % 2 == 0 and i > 0:
            keyboard.append(row)
            row = []
        row.append(InlineKeyboardButton(bank, callback_data=f"select_bank_{i}"))
    
    # Add user's custom banks if any
    if user_id in user_custom_banks and user_custom_banks[user_id]:
        # Add a separator row if there are default banks
        if row:
            keyboard.append(row)
            row = []
        
        # Add a header for custom banks
        keyboard.append([InlineKeyboardButton("--- Your Custom Banks ---", callback_data="custom_bank_header")])
        
        # Add custom banks
        for i, bank in enumerate(user_custom_banks[user_id]):
            if i % 2 == 0 and i > 0:
                keyboard.append(row)
                row = []
            # Use a different prefix for custom banks to distinguish them
            row.append(InlineKeyboardButton(bank, callback_data=f"select_custom_bank_{i}"))
    
    # Add option to enter a different bank name
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("Enter a different bank name", callback_data="enter_different_bank")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # If this is from a callback query, use edit_message_text
    if hasattr(update, 'callback_query'):
        update.callback_query.edit_message_text(
            text="🏦 Please select a bank or enter a different bank name:",
            reply_markup=reply_markup
        )
    else:
        # Otherwise, send a new message
        update.message.reply_text(
            "🏦 Please select a bank or enter a different bank name:",
            reply_markup=reply_markup
        )


def handle_conversation(update: Update, context) -> None:
    """Handle the conversation flow for collecting deposit information."""
    # Safely extract user_id and text from the update object
    if hasattr(update, 'effective_user') and update.effective_user is not None:
        user_id = update.effective_user.id
    elif hasattr(update, 'message') and hasattr(update.message, 'from_user') and update.message.from_user is not None:
        user_id = update.message.from_user.id
    else:
        logger.error("Could not determine user_id in handle_conversation")
        return
    
    # Safely extract text from the message
    if hasattr(update, 'message') and hasattr(update.message, 'text'):
        text = update.message.text
    else:
        logger.error("No text found in message")
        return
    
    if user_id not in user_states:
        # If no active conversation, process as a regular message
        collect_message(update, context)
        return
    
    state = user_states[user_id]['state']
    
    if state == 'waiting_for_custom_bank_name':
        # User is adding a custom bank
        bank_name = text.strip()
        
        # Initialize user's custom banks list if not already done
        if user_id not in user_custom_banks:
            user_custom_banks[user_id] = []
        
        # Check if bank already exists in default list or user's custom list
        if bank_name in NEPAL_BANKS:
            update.message.reply_text(
                f"❗ '{bank_name}' already exists in the default bank list. Please enter a different name:"
            )
            return
        elif bank_name in user_custom_banks[user_id]:
            update.message.reply_text(
                f"❗ '{bank_name}' already exists in your custom bank list. Please enter a different name:"
            )
            return
        
        # Add the custom bank
        user_custom_banks[user_id].append(bank_name)
        
        # Clear the conversation state
        del user_states[user_id]
        
        update.message.reply_text(
            f"✅ Custom bank '{bank_name}' has been added to your list."
            f"\n\nYou can now use this bank in deposit entry and limit check features."
            f"\n\nUse /settings to access banking features."
        )
        return
    
    elif state == 'waiting_for_deposit_amount':
        # Try to parse the deposit amount
        try:
            # Remove any currency symbols and convert to float
            numeric_str = re.sub(r'[€$£¥]', '', text)
            # Handle both decimal separators
            if ',' in numeric_str and '.' not in numeric_str:
                numeric_str = numeric_str.replace(',', '.')
            
            deposit_amount = float(numeric_str)
            user_states[user_id]['deposit_amount'] = deposit_amount
            user_states[user_id]['state'] = 'waiting_for_bank_name'
            
            update.message.reply_text(
                f"✅ Deposit amount recorded: {deposit_amount}\n\n"
                f"📝 Now, please enter the bank name:"
            )
        except ValueError:
            update.message.reply_text(
                "❗ Invalid amount. Please enter a valid number for the deposit amount:"
            )
    
    elif state == 'waiting_for_bank_name':
        # Store the bank name
        user_states[user_id]['bank_name'] = text
        user_states[user_id]['state'] = 'waiting_for_remaining_balance'
        
        # Ask for remaining balance
        update.message.reply_text(
            f"✅ Bank name recorded: {text}\n\n"
            f"📝 Now, please enter the remaining balance (or type '0' if none):"
        )
    
    elif state == 'waiting_for_remaining_balance':
        # Try to parse the remaining balance
        try:
            # Remove any currency symbols and convert to float
            numeric_str = re.sub(r'[€$£¥]', '', text)
            # Handle both decimal separators
            if ',' in numeric_str and '.' not in numeric_str:
                numeric_str = numeric_str.replace(',', '.')
            
            remaining_balance = float(numeric_str)
            user_states[user_id]['remaining_balance'] = remaining_balance
            user_states[user_id]['state'] = 'waiting_for_csv_path'
            
            # Ask for CSV file path or use default
            update.message.reply_text(
                f"✅ Remaining balance recorded: {remaining_balance}\n\n"
                f"📝 Do you want to append to an existing CSV file?\n"
                f"1. Yes - I'll provide the file path\n"
                f"2. No - Create a new file (default)\n\n"
                f"Please reply with '1' or '2', or enter the full path to your CSV file:"
            )
        except ValueError:
            update.message.reply_text(
                "❗ Invalid number format. Please enter a valid number for the remaining balance:"
            )
            return
    
    elif state == 'waiting_for_csv_path':
        if text == '1':
            user_states[user_id]['state'] = 'waiting_for_csv_path_input'
            update.message.reply_text(
                "📝 Please enter the full path to your CSV file (e.g., C:\\Users\\YourName\\Documents\\my_file.csv):"
            )
        elif text == '2' or text.lower() in ['no', 'default', 'new']:
            # Use default filename (no CSV path)
            user_states[user_id]['csv_path'] = None
            # Make sure we're using the message object, not the update directly
            if hasattr(update, 'callback_query'):
                # If this was triggered from a callback query
                process_export_csv(update, context, use_manual_input=True)
            else:
                # If this was triggered from a text message
                try:
                    process_export_csv(update, context, use_manual_input=True)
                except Exception as e:
                    logger.error(f"Error processing CSV export: {e}")
                    update.message.reply_text(f"❗ Error creating CSV file: {str(e)}")
                    # Clear the conversation state on error
                    if user_id in user_states:
                        del user_states[user_id]
        elif os.path.isfile(text) and text.lower().endswith('.csv'):
            # User provided a valid CSV path directly
            user_states[user_id]['csv_path'] = text
            try:
                process_export_csv(update, context, use_manual_input=True)
            except Exception as e:
                logger.error(f"Error processing CSV export: {e}")
                update.message.reply_text(f"❗ Error creating CSV file: {str(e)}")
                # Clear the conversation state on error
                if user_id in user_states:
                    del user_states[user_id]
        else:
            update.message.reply_text(
                "❗ Invalid choice. Please reply with '1', '2', or a valid CSV file path:\n"
                "1. Yes - I'll provide the file path\n"
                "2. No - Create a new file (default)"
            )
    
    elif state == 'waiting_for_csv_path_input':
        if os.path.isfile(text) and text.lower().endswith('.csv'):
            user_states[user_id]['csv_path'] = text
            process_export_csv(update, context, use_manual_input=True)
        else:
            update.message.reply_text(
                "❗ Invalid file path or file doesn't exist. Please enter a valid CSV file path:"
            )

def export_csv(update: Update, context) -> None:
    """Start the process of exporting results as a CSV file with manual input option."""
    # Safely extract user_id and message from the update object
    if hasattr(update, 'effective_user') and update.effective_user is not None:
        user_id = update.effective_user.id
        message = update.message
    elif hasattr(update, 'message') and hasattr(update.message, 'from_user') and update.message.from_user is not None:
        user_id = update.message.from_user.id
        message = update.message
    else:
        logger.error("Could not determine user_id in export_csv")
        return

    if user_id not in user_messages or not user_messages[user_id]:
        message.reply_text("❗ No messages collected yet. Forward some messages first.")
        return
    
    # Ask user if they want to use simple export or detailed export
    keyboard = [
        [InlineKeyboardButton("Simple Export", callback_data='csv_simple_export')],
        [InlineKeyboardButton("Detailed Export", callback_data='csv_detailed_export')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message.reply_text(
        "📊 CSV Export Options:\n\n"
        "Choose your export format:\n"
        "• Simple: Just amounts, charges, and running sums\n"
        "• Detailed: Full format with bank details and balance",
        reply_markup=reply_markup
    )

def export_simple_csv(update: Update, context) -> None:
    """Export the results as a simple CSV file with amounts, charges, and running totals in a clearer format."""
    # Determine if this is called from a callback query or directly
    if hasattr(update, 'callback_query'):
        query = update.callback_query
        user_id = query.from_user.id
        message = query.message
    else:
        user_id = update.effective_user.id
        message = update.message

    if user_id not in user_messages or not user_messages[user_id]:
        message.reply_text("❗ No messages collected yet. Forward some messages first.")
        return

    # Get user preferences
    preferences = user_preferences.get(user_id, DEFAULT_PREFERENCES.copy())
    decimal_separator = preferences['decimal_separator']

    # Create the appropriate pattern based on user preference
    if decimal_separator == '.':
        pattern = r'([€$£¥])?(\-?\d+(?:\.\d+)?)'
    else:
        pattern = r'([€$£¥])?(\-?\d+(?:,\d+)?)'

    amounts = []  # Values > AMOUNT_THRESHOLD
    charges = []  # Values ≤ AMOUNT_THRESHOLD
    
    # Process all collected messages
    for message_data in user_messages[user_id]:
        message_text = message_data['text']

        # Find all matches in the message
        matches = re.findall(pattern, message_text)

        for match in matches:
            currency, original_number, processed_number, value, has_decimal = extract_number_value(match, decimal_separator, message_text)

            # Format the extracted value
            extracted_value = f"{currency}{processed_number}" if currency and preferences['include_currency'] else processed_number

            # Add to appropriate category
            if value > AMOUNT_THRESHOLD:
                amounts.append(extracted_value)
            else:
                charges.append(extracted_value)

    if not amounts and not charges:
        message.reply_text(
            f"❗ I couldn't find any numbers in your collected messages."
        )
        return

    # Create a new CSV file with an improved format
    current_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(current_dir, f"simple_export_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header with three columns
            writer.writerow(['Amount', 'Charge', 'Running Total'])
            
            # Prepare data for export
            max_rows = max(len(amounts), len(charges))
            running_total = 0
            
            # Write data row by row
            for i in range(max_rows):
                amount_value = ""
                charge_value = ""
                amount_numeric = 0
                charge_numeric = 0
                
                # Get amount if available
                if i < len(amounts):
                    amount_value = amounts[i]
                    # Extract numeric value
                    numeric_str = re.sub(r'[€$£¥]', '', amount_value)
                    if decimal_separator == ',':
                        numeric_str = numeric_str.replace(',', '.')
                    try:
                        amount_numeric = float(numeric_str)
                    except ValueError:
                        amount_numeric = 0
                
                # Get charge if available
                if i < len(charges):
                    charge_value = charges[i]
                    # Extract numeric value
                    numeric_str = re.sub(r'[€$£¥]', '', charge_value)
                    if decimal_separator == ',':
                        numeric_str = numeric_str.replace(',', '.')
                    try:
                        charge_numeric = float(numeric_str)
                    except ValueError:
                        charge_numeric = 0
                
                # Update running total
                running_total += amount_numeric - charge_numeric
                
                # Write the row
                writer.writerow([amount_value, charge_value, f"{running_total:.2f}"])
            
            # Write total row
            writer.writerow(['', '', ''])
            writer.writerow(['TOTAL', '', f"{running_total:.2f}"])

        # Send the file to the user
        with open(filename, 'rb') as file:
            message.reply_document(
                document=file,
                filename=os.path.basename(filename),
                caption=f"📊 Simple CSV export with improved format.\n\nThe file includes:\n- Amounts in the first column\n- Charges in the second column\n- Running total in the third column (adds amounts, subtracts charges)\n- Final total at the bottom"
            )

        # Remove the temporary file
        os.remove(filename)

    except Exception as e:
        logger.error(f"Error exporting simple CSV: {e}")
        message.reply_text(
            f"❗ Sorry, there was an error creating your CSV file: {str(e)}"
        )

def process_export_csv(update: Update, context, use_manual_input=False) -> None:
    """Export the results as a CSV file with the format: Date, Deposit Amount, Bank Name, Paid To Host, Total Deposit, Total Paid, Balance.
    Maintains a running balance by using the previous day's remaining balance as today's starting balance."""
    # Determine if this is called from a callback query or directly
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        query = update.callback_query
        user_id = query.from_user.id
        message = query.message
    else:
        # Handle the case when update.effective_user might be None
        if hasattr(update, 'effective_user') and update.effective_user is not None:
            user_id = update.effective_user.id
            message = update.message
        elif hasattr(update, 'message') and update.message is not None:
            user_id = update.message.from_user.id
            message = update.message
        else:
            # Fallback for when we can't determine the user_id
            logger.error("Could not determine user_id from update object")
            return

    if user_id not in user_messages or not user_messages[user_id]:
        message.reply_text("❗ No messages collected yet. Forward some messages first.")
        return

    # Get user preferences
    preferences = user_preferences.get(user_id, DEFAULT_PREFERENCES.copy())
    decimal_separator = preferences['decimal_separator']

    # Create the appropriate pattern based on user preference
    if decimal_separator == '.':
        pattern = r'([€$£¥])?(\-?\d+(?:\.\d+)?)'
    else:
        pattern = r'([€$£¥])?(\-?\d+(?:,\d+)?)'

    amounts = []  # Values > AMOUNT_THRESHOLD (Deposit Amount)
    charges = []  # Values ≤ AMOUNT_THRESHOLD (Paid To Host)
    
    # Get the current date for the report
    current_date = datetime.now().strftime('%m/%d/%Y')
    
    # Use manual input if requested
    if use_manual_input and user_id in user_states:
        deposit_amount = user_states[user_id].get('deposit_amount')
        bank_name = user_states[user_id].get('bank_name')
        csv_path = user_states[user_id].get('csv_path')
        manual_remaining_balance = user_states[user_id].get('remaining_balance')
        
        # Format the deposit amount
        if deposit_amount is not None:
            # Convert to string with appropriate format
            deposit_str = str(int(deposit_amount) if deposit_amount.is_integer() else deposit_amount)
            amounts = [deposit_str] + amounts  # Add manual deposit as first amount
    else:
        bank_name = "Remaining Balance"  # Default bank name
        csv_path = None
        manual_remaining_balance = None
        
    # Variable to store previous day's balance
    previous_balance = 0.0
    
    # If user manually entered a remaining balance, use it instead of reading from file
    if manual_remaining_balance is not None:
        previous_balance = manual_remaining_balance
        logger.info(f"Using manually entered remaining balance: {previous_balance}")
    

    # Process all collected messages if not using manual input exclusively
    if not use_manual_input or not amounts:
        for message_data in user_messages[user_id]:
            message_text = message_data['text']

            # Find all matches in the message
            matches = re.findall(pattern, message_text)

            for match in matches:
                currency, original_number, processed_number, value, has_decimal = extract_number_value(match, decimal_separator, message_text)

                # Format the extracted value
                extracted_value = f"{currency}{processed_number}" if currency and preferences['include_currency'] else processed_number

                # Add to appropriate category
                if value > AMOUNT_THRESHOLD:
                    amounts.append(extracted_value)
                else:
                    charges.append(extracted_value)

    if not amounts and not charges:
        message.reply_text(
            f"❗ I couldn't find any numbers in your collected messages."
        )
        return

    # Calculate the total deposit (sum of amounts)
    total_deposit = 0
    for amount_str in amounts:
        # Remove any currency symbol
        numeric_str = re.sub(r'[€$£¥]', '', amount_str)
        # Replace comma with period if needed
        if decimal_separator == ',':
            numeric_str = numeric_str.replace(',', '.')
        # Convert to float and add to sum
        try:
            total_deposit += float(numeric_str)
        except ValueError:
            # Skip if conversion fails
            pass
    
    # Calculate the total paid (sum of charges)
    total_paid = 0
    for charge_str in charges:
        # Remove any currency symbol
        numeric_str = re.sub(r'[€$£¥]', '', charge_str)
        # Replace comma with period if needed
        if decimal_separator == ',':
            numeric_str = numeric_str.replace(',', '.')
        # Convert to float and add to sum
        try:
            total_paid += float(numeric_str)
        except ValueError:
            # Skip if conversion fails
            pass
    
    # Calculate the balance (total deposit - total paid)
    balance = total_deposit - total_paid
    
    # Format the totals to match the screenshot format - always show 2 decimal places
    total_deposit_str = f"{total_deposit:.2f}"
    total_paid_str = f"{total_paid:.2f}"
    balance_str = f"{balance:.2f}"

    # Determine the CSV file path
    if csv_path and os.path.isfile(csv_path):
        filename = csv_path
        file_exists = True
        # Store the path for future use
        user_csv_files[user_id] = csv_path
    else:
        # Create a new CSV file with the requested format
        # Make sure to use an absolute path for the new file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(current_dir, f"decimal_stripper_export_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        file_exists = False
        # Store the path for future use
        user_csv_files[user_id] = filename

    try:
        if file_exists:
            # Read existing file to get current totals and previous day's balance
            existing_totals = {'total_deposit': 0, 'total_paid': 0, 'balance': 0}
            try:
                with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = list(reader)
                    
                    # Check if file has the expected format
                    if len(rows) > 0 and 'Date' in rows[0] and 'Total Deposit' in rows[0]:
                        # Find the totals row (usually the last non-empty row)
                        for row in reversed(rows):
                            if row and row[4] and row[5] and row[6]:  # Total columns have values
                                try:
                                    existing_totals['total_deposit'] = float(row[4])
                                    existing_totals['total_paid'] = float(row[5])
                                    existing_totals['balance'] = float(row[6])
                                    previous_balance = existing_totals['balance']  # Set previous day's balance
                                    break
                                except ValueError:
                                    pass
            except Exception as e:
                logger.error(f"Error reading existing CSV: {e}")
                # Continue with new file if reading fails
                file_exists = False
            
            # Open file in append mode
            mode = 'a'
            
            # If we have a previous balance, handle it appropriately
            if previous_balance > 0:
                if use_manual_input and amounts:
                    # If user manually entered a deposit and remaining balance, inform them
                    if user_id in user_states and user_states[user_id].get('remaining_balance') is not None:
                        message.reply_text(f"Note: Using your manually entered remaining balance of {previous_balance}. This will be included in your total calculations.")
                    else:
                        # Using previous balance from file
                        message.reply_text(f"Note: Previous day's remaining balance was {previous_balance}. This will be included in your total calculations.")
                else:
                    # Add previous balance as the first amount if no manual deposit was specified
                    amounts = [str(previous_balance)] + amounts
                    bank_name = "Previous Balance"  # Mark this as previous balance
            
            # Calculate new totals
            # Always include previous balance in calculations
            total_deposit += existing_totals['total_deposit']
            total_paid += existing_totals['total_paid']
            
            # Calculate the final balance
            balance = total_deposit - total_paid
            
            # Log the balance calculation for debugging
            logger.debug(f"Balance calculation: {total_deposit} - {total_paid} = {balance}")
            logger.debug(f"Previous balance: {previous_balance}")
            
            # Inform user about the running balance
            if previous_balance > 0:
                message.reply_text(f"Your running balance includes the previous day's balance of {previous_balance}.")

        else:
            # Create new file
            mode = 'w'
        
        with open(filename, mode, newline='', encoding='utf-8') as csvfile:
            # Create writer
            writer = csv.writer(csvfile)
            
            # Write header if creating a new file
            if not file_exists:
                fieldnames = ['Date', 'Deposit Amount', 'Bank Name', 'Paid To Host', 'Total Deposit', 'Total Paid', 'Remaining Balance']
                writer.writerow(fieldnames)
                # Add a separator row for better readability
                writer.writerow(['---', '---', '---', '---', '---', '---', '---'])
            
            # Format the deposit amount with two decimal places if it's a number
            deposit_amount_formatted = ''
            if amounts and amounts[0]:
                try:
                    # Remove any currency symbol and convert to float
                    numeric_str = re.sub(r'[€$£¥]', '', amounts[0])
                    # Handle both decimal separators
                    if decimal_separator == ',':
                        numeric_str = numeric_str.replace(',', '.')
                    # Always format with 2 decimal places for consistency
                    deposit_amount_formatted = f"{float(numeric_str):.2f}"
                except ValueError:
                    deposit_amount_formatted = amounts[0]
            
            # Write the deposit information in the first row
            deposit_row = [current_date, deposit_amount_formatted, bank_name, '', '', '', '']
            writer.writerow(deposit_row)
            
            # Write the charges (payments) in subsequent rows
            for i in range(len(charges)):
                # Format the charge with two decimal places if it's a number
                charge_formatted = ''
                try:
                    # Remove any currency symbol and convert to float
                    numeric_str = re.sub(r'[€$£¥]', '', charges[i])
                    # Handle both decimal separators
                    if decimal_separator == ',':
                        numeric_str = numeric_str.replace(',', '.')
                    charge_formatted = f"{float(numeric_str):.2f}"
                except ValueError:
                    charge_formatted = charges[i]
                
                # For each charge, create a row with the payment in the Paid To Host column
                charge_row = ['', '', '', charge_formatted, '', '', '']
                writer.writerow(charge_row)
            
            # Add empty row before totals
            writer.writerow(['', '', '', '', '', '', ''])
            
            # Calculate running totals for the bottom row
            # Format the totals with two decimal places
            total_deposit_formatted = f"{total_deposit:.2f}"
            total_paid_formatted = f"{total_paid:.2f}"
            balance_formatted = f"{balance:.2f}"
            
            # Add a separator row before totals for better readability
            writer.writerow(['---', '---', '---', '---', '---', '---', '---'])
            
            # Write the totals row at the bottom with proper labels
            totals_row = ['', 'SUMMARY', '', 'TOTALS:', total_deposit_formatted, total_paid_formatted, balance_formatted]
            writer.writerow(totals_row)

        # Send the file to the user
        with open(filename, 'rb') as file:
            message.reply_document(
                document=file,
                filename=os.path.basename(filename),
                caption=f"📊 CSV export with the format: Date, Deposit Amount, Bank Name, Paid To Host, Total Deposit, Total Paid, Remaining Balance.\n\nThe file includes:\n- Deposit information in the first row\n- Individual payments in the Paid To Host column\n- Running totals at the bottom\n- Your manually entered remaining balance or previous day's balance is included in calculations\n- Improved formatting for better readability"
            )

        # Don't remove the file if it's a user-specified path
        if not csv_path:
            os.remove(filename)

        # Clear the conversation state
        if user_id in user_states:
            del user_states[user_id]

    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        message.reply_text(
            f"❗ Sorry, there was an error creating your CSV file: {str(e)}"
        )
        
        # Clear the conversation state on error
        if user_id in user_states:
            del user_states[user_id]

def export_json(update: Update, context) -> None:
    """Export the results as a JSON file with 3 columns: amounts, charges, and sum."""
    user_id = update.effective_user.id

    if user_id not in user_messages or not user_messages[user_id]:
        update.message.reply_text("❗ No messages collected yet. Forward some messages first.")
        return

    # Get user preferences
    preferences = user_preferences.get(user_id, DEFAULT_PREFERENCES.copy())
    decimal_separator = preferences['decimal_separator']

    # Create the appropriate pattern based on user preference
    if decimal_separator == '.':
        pattern = r'([€$£¥])?(\-?\d+(?:\.\d+)?)'
    else:
        pattern = r'([€$£¥])?(\-?\d+(?:,\d+)?)'

    amounts = []
    charges = []

    # Process all collected messages
    for message_data in user_messages[user_id]:
        message_text = message_data['text']

        # Find all matches in the message
        matches = re.findall(pattern, message_text)

        for match in matches:
            currency, original_number, processed_number, value, has_decimal = extract_number_value(match, decimal_separator, message_text)

            # Format the extracted value
            extracted_value = f"{currency}{processed_number}" if currency and preferences['include_currency'] else processed_number

            # Add to appropriate category
            if value > AMOUNT_THRESHOLD:
                amounts.append(extracted_value)
            else:
                charges.append(extracted_value)

    if not amounts and not charges:
        update.message.reply_text(
            f"❗ I couldn't find any numbers in your collected messages."
        )
        return

    # Calculate the sum of all values
    total_sum = 0
    for value_list in [amounts, charges]:
        for value_str in value_list:
            # Remove any currency symbol
            numeric_str = re.sub(r'[€$£¥]', '', value_str)
            # Replace comma with period if needed
            if decimal_separator == ',':
                numeric_str = numeric_str.replace(',', '.')
            # Convert to float and add to sum
            try:
                total_sum += float(numeric_str)
            except ValueError:
                # Skip if conversion fails
                pass

    # Create simplified export data with 3 columns
    export_data = {
        'Amounts': amounts,
        'Charges': charges,
        'Total Sum': total_sum
    }
    
    # Create a JSON file
    filename = f"decimal_stripper_export_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    try:
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(export_data, jsonfile, indent=4, ensure_ascii=False)

        # Send the file to the user
        with open(filename, 'rb') as file:
            update.message.reply_document(
                document=file,
                filename=filename,
                caption=f"📊 JSON export with {len(amounts)} amounts, {len(charges)} charges, and their sum."
            )

        # Clean up the file
        os.remove(filename)

    except Exception as e:
        logger.error(f"Error exporting JSON: {e}")
        update.message.reply_text(
            "❗ Sorry, there was an error creating your JSON file. Please try again later."
        )

def clear_command(update: Update, context) -> None:
    """Clear all collected messages for the user."""
    user_id = update.effective_user.id
    
    # Reset the message collection for this user
    user_messages[user_id] = []
    
    update.message.reply_text(
        "✅ Your collection has been cleared. You can start forwarding new messages now."
    )

def settings_command(update: Update, context) -> None:
    """Show and allow changing user preferences."""
    user_id = update.effective_user.id

    # Initialize user preferences if not already set
    if user_id not in user_preferences:
        user_preferences[user_id] = DEFAULT_PREFERENCES.copy()

    preferences = user_preferences[user_id]

    # Create inline keyboard for settings
    keyboard = [
        [
            InlineKeyboardButton("Decimal: .", callback_data="set_decimal_."),
            InlineKeyboardButton("Decimal: ,", callback_data="set_decimal_,")
        ],
        [
            InlineKeyboardButton(
                "Currency: " + ("ON ✅" if preferences['include_currency'] else "OFF ❌"),
                callback_data="toggle_currency"
            )
        ],
        [
            InlineKeyboardButton("Format: Simple", callback_data="set_format_simple"),
            InlineKeyboardButton("Format: Detailed", callback_data="set_format_detailed")
        ],
        [
            InlineKeyboardButton(
                "Silent collection: " + ("ON ✅" if preferences['silent_collection'] else "OFF ❌"),
                callback_data="toggle_silent"
            )
        ],
        [
            InlineKeyboardButton("🏦 Bank Deposit Entry", callback_data="bank_deposit_entry")
        ],
        [
            InlineKeyboardButton("💰 Check Remaining Limit", callback_data="check_remaining_limit")
        ],this
        [
            InlineKeyboardButton("➕ Add Custom Bank", callback_data="add_custom_bank")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    current_settings = (
        f"⚙️ <b>Current Settings</b>\n\n"
        f"🔢 Decimal Separator: '{preferences['decimal_separator']}'\n"
        f"💱 Include Currency: {'Yes' if preferences['include_currency'] else 'No'}\n"
        f"📋 Output Format: {preferences['output_format'].capitalize()}\n"
        f"🔕 Silent Collection: {'Yes' if preferences['silent_collection'] else 'No'}\n\n"
        f"Click below to change settings or use banking features:"
    )

    update.message.reply_text(
        current_settings,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

def button_callback(update: Update, context) -> None:
    """Handle button presses from inline keyboards."""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data

    # Initialize user preferences if not already set
    if user_id not in user_preferences:
        user_preferences[user_id] = DEFAULT_PREFERENCES.copy()

    # Handle different button actions
    if data.startswith('set_decimal_'):
        user_preferences[user_id]['decimal_separator'] = data[-1]
    elif data == 'toggle_currency':
        user_preferences[user_id]['include_currency'] = not user_preferences[user_id]['include_currency']
    elif data.startswith('set_format_'):
        user_preferences[user_id]['output_format'] = data[11:]
    elif data == 'toggle_silent':
        user_preferences[user_id]['silent_collection'] = not user_preferences[user_id]['silent_collection']
    elif data == 'csv_simple_export':
        # User wants a simple CSV with just amounts, charges and running sums
        query.edit_message_text(text="Processing simple CSV export...")
        export_simple_csv(update, context)
        return
    elif data == 'csv_detailed_export':
        # User wants the detailed CSV export
        keyboard = [
            [InlineKeyboardButton("Yes, I'll enter details", callback_data='csv_manual_input')],
            [InlineKeyboardButton("No, use extracted data only", callback_data='csv_auto_export')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        query.edit_message_text(
            "Do you want to manually enter deposit amount and bank name?",
            reply_markup=reply_markup
        )
        return
    elif data == 'csv_manual_input':
        # User wants to manually enter deposit information
        query.edit_message_text(text="Starting manual input process...")
        # Start the conversation flow for manual input
        ask_for_deposit_info(update, context)
        return
    elif data == 'csv_auto_export':
        # User wants to use only extracted data
        query.edit_message_text(text="Processing CSV export with extracted data...")
        process_export_csv(update, context, use_manual_input=False)
        return
    elif data == 'bank_deposit_entry':
        # User wants to manually enter bank deposit information
        query.edit_message_text(text="Starting bank deposit entry process...")
        start_bank_deposit_entry(update, context)
        return
    elif data == 'check_remaining_limit':
        # User wants to check remaining limit for a bank
        query.edit_message_text(text="Starting remaining limit check process...")
        start_remaining_limit_check(update, context)
        return
    elif data == 'add_custom_bank':
        # User wants to add a custom bank
        query.edit_message_text(text="Starting custom bank addition process...")
        start_add_custom_bank(update, context)
        return
    elif data.startswith('select_bank_') or data.startswith('select_custom_bank_'):
        # User selected a bank from the list (either default or custom)
        if data.startswith('select_bank_'):
            # Default bank selected
            bank_index = int(data.split('_')[-1])
            if 0 <= bank_index < len(NEPAL_BANKS):
                selected_bank = NEPAL_BANKS[bank_index]
                user_states[user_id]['selected_bank'] = selected_bank
        else:
            # Custom bank selected
            bank_index = int(data.split('_')[-1])
            if user_id in user_custom_banks and 0 <= bank_index < len(user_custom_banks[user_id]):
                selected_bank = user_custom_banks[user_id][bank_index]
                user_states[user_id]['selected_bank'] = selected_bank
            else:
                query.answer("Error: Custom bank not found")
                return
        
        # Process based on the action
        if user_states[user_id].get('action') == 'deposit_entry':
            query.edit_message_text(text=f"Selected bank: {selected_bank}\n\nPlease enter the deposit amount:")
            user_states[user_id]['state'] = 'waiting_for_deposit_amount'
        elif user_states[user_id].get('action') == 'limit_check':
            query.edit_message_text(text=f"Selected bank: {selected_bank}\n\nPlease enter the limit amount for this bank:")
            user_states[user_id]['state'] = 'waiting_for_limit_amount'
        elif user_states[user_id].get('action') == 'csv_export':
            # For CSV export, store the bank name and ask for deposit amount
            user_states[user_id]['bank_name'] = selected_bank
            query.edit_message_text(text=f"Selected bank: {selected_bank}\n\nPlease enter the deposit amount:")
            user_states[user_id]['state'] = 'waiting_for_deposit_amount'
        return
        
    elif data == 'enter_different_bank':
        # User wants to enter a custom bank name for this transaction
        query.edit_message_text(text="Please enter the bank name in your next message:")
        user_states[user_id]['state'] = 'waiting_for_bank_name'
        return

    # Get updated preferences
    preferences = user_preferences[user_id]

    # Update the settings message with new keyboard
    keyboard = [
        [
            InlineKeyboardButton("Decimal: .", callback_data="set_decimal_."),
            InlineKeyboardButton("Decimal: ,", callback_data="set_decimal_,")
        ],
        [
            InlineKeyboardButton(
                "Currency: " + ("ON ✅" if preferences['include_currency'] else "OFF ❌"),
                callback_data="toggle_currency"
            )
        ],
        [
            InlineKeyboardButton("Format: Simple", callback_data="set_format_simple"),
            InlineKeyboardButton("Format: Detailed", callback_data="set_format_detailed")
        ],
        [
            InlineKeyboardButton(
                "Silent collection: " + ("ON ✅" if preferences['silent_collection'] else "OFF ❌"),
                callback_data="toggle_silent"
            )
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    # Create updated settings text
    current_settings = (
        f"⚙️ <b>Current Settings</b>\n\n"
        f"🔢 Decimal Separator: '{preferences['decimal_separator']}'\n"
        f"💱 Include Currency: {'Yes' if preferences['include_currency'] else 'No'}\n"
        f"📋 Output Format: {preferences['output_format'].capitalize()}\n"
        f"🔕 Silent Collection: {'Yes' if preferences['silent_collection'] else 'No'}\n\n"
        f"Click below to change settings:"
    )

    try:
        # Edit the message with updated settings
        query.edit_message_text(
            text=current_settings,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    except Exception as e:
        # Message is not modified, ignore the error
        logger.error(f"Error updating settings message: {e}")

    # Answer the callback query to remove the loading state
    query.answer(f"Setting updated: {data}")

def stats_command(update: Update, context) -> None:
    """Show statistics about collected messages."""
    user_id = update.effective_user.id

    if user_id not in user_messages or not user_messages[user_id]:
        update.message.reply_text("❗ No messages collected yet. Forward some messages first.")
        return

    # Get user preferences
    preferences = user_preferences.get(user_id, DEFAULT_PREFERENCES.copy())
    decimal_separator = preferences['decimal_separator']

    # Create the appropriate pattern based on user preference
    if decimal_separator == '.':
        pattern = r'([€$£¥])?(\-?\d+(?:\.\d+)?)'
    else:
        pattern = r'([€$£¥])?(\-?\d+(?:,\d+)?)'

    total_messages = len(user_messages[user_id])
    total_numbers = 0
    amounts_count = 0
    charges_count = 0
    decimal_numbers_count = 0
    whole_numbers_count = 0

    # Process all collected messages
    for message_data in user_messages[user_id]:
        message_text = message_data['text']

        # Find all matches in the message
        matches = re.findall(pattern, message_text)
        total_numbers += len(matches)

        for match in matches:
            currency, original_number, processed_number, value, has_decimal = extract_number_value(match, decimal_separator, message_text)

            # Count by category
            if value > AMOUNT_THRESHOLD:
                amounts_count += 1
            else:
                charges_count += 1

            # Count by number type
            if has_decimal:
                decimal_numbers_count += 1
            else:
                whole_numbers_count += 1

    # Create stats message
    stats_message = (
        f"📊 <b>Collection Statistics</b>\n\n"
        f"📱 Total Messages: {total_messages}\n"
        f"🔢 Total Numbers Found: {total_numbers}\n"
        f"💰 Amounts (>{AMOUNT_THRESHOLD}): {amounts_count} - decimal parts stripped\n"
        f"💸 Charges (≤{AMOUNT_THRESHOLD}): {charges_count} - kept exactly as found\n\n"
        f"🔍 Numbers with Decimal Part: {decimal_numbers_count}\n"
        f"🔍 Whole Numbers: {whole_numbers_count}\n\n"
        f"Use /process to see the actual values."
    )

    update.message.reply_text(stats_message, parse_mode='HTML')

def start_bank_deposit_entry(update: Update, context) -> None:
    """Start the process of entering a bank deposit manually."""
    user_id = update.callback_query.from_user.id
    
    # Initialize user state
    user_states[user_id] = {
        'state': 'selecting_bank',
        'action': 'deposit_entry'
    }
    
    # Create a keyboard with Nepali banks and user's custom banks
    keyboard = []
    row = []
    
    # Add default Nepali banks
    for i, bank in enumerate(NEPAL_BANKS):
        if i % 2 == 0 and i > 0:
            keyboard.append(row)
            row = []
        row.append(InlineKeyboardButton(bank, callback_data=f"select_bank_{i}"))
    
    # Add user's custom banks if any
    if user_id in user_custom_banks and user_custom_banks[user_id]:
        # Add a separator row if there are default banks
        if row:
            keyboard.append(row)
            row = []
        
        # Add a header for custom banks
        keyboard.append([InlineKeyboardButton("--- Your Custom Banks ---", callback_data="custom_bank_header")])
        
        # Add custom banks
        for i, bank in enumerate(user_custom_banks[user_id]):
            if i % 2 == 0 and i > 0:
                keyboard.append(row)
                row = []
            # Use a different prefix for custom banks to distinguish them
            row.append(InlineKeyboardButton(bank, callback_data=f"select_custom_bank_{i}"))
    
    if row:  # Add any remaining buttons
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.callback_query.edit_message_text(
        "🏦 Please select a bank:",
        reply_markup=reply_markup
    )

def start_remaining_limit_check(update: Update, context) -> None:
    """Start the process of checking remaining limit for a bank."""
    user_id = update.callback_query.from_user.id
    
    # Initialize user state
    user_states[user_id] = {
        'state': 'selecting_bank',
        'action': 'limit_check'
    }
    
    # Create a keyboard with Nepali banks and user's custom banks
    keyboard = []
    row = []
    
    # Add default Nepali banks
    for i, bank in enumerate(NEPAL_BANKS):
        if i % 2 == 0 and i > 0:
            keyboard.append(row)
            row = []
        row.append(InlineKeyboardButton(bank, callback_data=f"select_bank_{i}"))
    
    # Add user's custom banks if any
    if user_id in user_custom_banks and user_custom_banks[user_id]:
        # Add a separator row if there are default banks
        if row:
            keyboard.append(row)
            row = []
        
        # Add a header for custom banks
        keyboard.append([InlineKeyboardButton("--- Your Custom Banks ---", callback_data="custom_bank_header")])
        
        # Add custom banks
        for i, bank in enumerate(user_custom_banks[user_id]):
            if i % 2 == 0 and i > 0:
                keyboard.append(row)
                row = []
            # Use a different prefix for custom banks to distinguish them
            row.append(InlineKeyboardButton(bank, callback_data=f"select_custom_bank_{i}"))
    
    if row:  # Add any remaining buttons
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.callback_query.edit_message_text(
        "🏦 Please select a bank to check remaining limit:",
        reply_markup=reply_markup
    )

def start_add_custom_bank(update: Update, context) -> None:
    """Start the process of adding a custom bank."""
    user_id = update.callback_query.from_user.id
    
    # Initialize user state
    user_states[user_id] = {
        'state': 'waiting_for_custom_bank_name',
        'action': 'add_custom_bank'
    }
    
    update.callback_query.edit_message_text(
        "🏦 Please enter the name of the custom bank you want to add:"
    )

def handle_conversation(update: Update, context) -> None:
    """Handle the conversation flow for collecting deposit information."""
    # Safely extract user_id and text from the update object
    if hasattr(update, 'effective_user') and update.effective_user is not None:
        user_id = update.effective_user.id
    elif hasattr(update, 'message') and hasattr(update.message, 'from_user') and update.message.from_user is not None:
        user_id = update.message.from_user.id
    else:
        logger.error("Could not determine user_id in handle_conversation")
        return
    
    # Safely extract text from the message
    if hasattr(update, 'message') and hasattr(update.message, 'text'):
        text = update.message.text
    else:
        logger.error("No text found in message")
        return
    
    if user_id not in user_states:
        # If no active conversation, process as a regular message
        collect_message(update, context)
        return
    
    state = user_states[user_id]['state']
    
    if state == 'waiting_for_custom_bank_name':
        # User is adding a custom bank
        bank_name = text.strip()
        
        # Initialize user's custom banks list if not already done
        if user_id not in user_custom_banks:
            user_custom_banks[user_id] = []
        
        # Check if bank already exists in default list or user's custom list
        if bank_name in NEPAL_BANKS:
            update.message.reply_text(
                f"❗ '{bank_name}' already exists in the default bank list. Please enter a different name:"
            )
            return
        elif bank_name in user_custom_banks[user_id]:
            update.message.reply_text(
                f"❗ '{bank_name}' already exists in your custom bank list. Please enter a different name:"
            )
            return
        
        # Add the custom bank
        user_custom_banks[user_id].append(bank_name)
        
        # Clear the conversation state
        del user_states[user_id]
        
        update.message.reply_text(
            f"✅ Custom bank '{bank_name}' has been added to your list."
            f"\n\nYou can now use this bank in deposit entry and limit check features."
            f"\n\nUse /settings to access banking features."
        )
        return
    
    elif state == 'waiting_for_deposit_amount':
        # Try to parse the deposit amount
        try:
            # Remove any currency symbols and convert to float
            numeric_str = re.sub(r'[€$£¥]', '', text)
            # Handle both decimal separators
            if ',' in numeric_str and '.' not in numeric_str:
                numeric_str = numeric_str.replace(',', '.')
            
            deposit_amount = float(numeric_str)
            user_states[user_id]['deposit_amount'] = deposit_amount
            
            # Check if this is for bank deposit entry or CSV export
            if user_states[user_id].get('action') == 'deposit_entry':
                selected_bank = user_states[user_id].get('selected_bank')
                
                # Initialize bank deposits for this user if not already done
                if user_id not in user_bank_deposits:
                    user_bank_deposits[user_id] = {}
                
                # Add to or update the deposit for this bank
                if selected_bank in user_bank_deposits[user_id]:
                    user_bank_deposits[user_id][selected_bank] += deposit_amount
                else:
                    user_bank_deposits[user_id][selected_bank] = deposit_amount
                
                update.message.reply_text(
                    f"✅ Deposit of {deposit_amount} recorded for {selected_bank}.\n\n"
                    f"Current total deposit for {selected_bank}: {user_bank_deposits[user_id][selected_bank]}"
                )
                
                # Clear the conversation state
                del user_states[user_id]
            else:
                # Continue with the CSV export flow
                user_states[user_id]['state'] = 'waiting_for_bank_name'
                
                update.message.reply_text(
                    f"✅ Deposit amount recorded: {deposit_amount}\n\n"
                    f"📝 Now, please enter the bank name:"
                )
        except ValueError:
            update.message.reply_text(
                "❗ Invalid amount. Please enter a valid number for the deposit amount:"
            )
    
    elif state == 'waiting_for_bank_name':
        # Store the bank name
        user_states[user_id]['bank_name'] = text
        user_states[user_id]['state'] = 'waiting_for_remaining_balance'
        
        # Ask for remaining balance
        update.message.reply_text(
            f"✅ Bank name recorded: {text}\n\n"
            f"📝 Now, please enter the remaining balance (or type '0' if none):"
        )
    
    elif state == 'waiting_for_remaining_balance':
        # Try to parse the remaining balance
        try:
            # Remove any currency symbols and convert to float
            numeric_str = re.sub(r'[€$£¥]', '', text)
            # Handle both decimal separators
            if ',' in numeric_str and '.' not in numeric_str:
                numeric_str = numeric_str.replace(',', '.')
            
            remaining_balance = float(numeric_str)
            user_states[user_id]['remaining_balance'] = remaining_balance
            user_states[user_id]['state'] = 'waiting_for_csv_path'
            
            # Ask for CSV file path or use default
            update.message.reply_text(
                f"✅ Remaining balance recorded: {remaining_balance}\n\n"
                f"📝 Do you want to append to an existing CSV file?\n"
                f"1. Yes - I'll provide the file path\n"
                f"2. No - Create a new file (default)\n\n"
                f"Please reply with '1' or '2', or enter the full path to your CSV file:"
            )
        except ValueError:
            update.message.reply_text(
                "❗ Invalid number format. Please enter a valid number for the remaining balance:"
            )
            return
    
    elif state == 'waiting_for_csv_path':
        if text == '1':
            user_states[user_id]['state'] = 'waiting_for_csv_path_input'
            update.message.reply_text(
                "📝 Please enter the full path to your CSV file (e.g., C:\\Users\\YourName\\Documents\\my_file.csv):"
            )
        elif text == '2' or text.lower() in ['no', 'default', 'new']:
            # Use default filename (no CSV path)
            user_states[user_id]['csv_path'] = None
            # Make sure we're using the message object, not the update directly
            if hasattr(update, 'callback_query'):
                # If this was triggered from a callback query
                process_export_csv(update, context, use_manual_input=True)
            else:
                # If this was triggered from a text message
                try:
                    process_export_csv(update, context, use_manual_input=True)
                except Exception as e:
                    logger.error(f"Error processing CSV export: {e}")
                    update.message.reply_text(f"❗ Error creating CSV file: {str(e)}")
                    # Clear the conversation state on error
                    if user_id in user_states:
                        del user_states[user_id]
        elif os.path.isfile(text) and text.lower().endswith('.csv'):
            # User provided a valid CSV path directly
            user_states[user_id]['csv_path'] = text
            try:
                process_export_csv(update, context, use_manual_input=True)
            except Exception as e:
                logger.error(f"Error processing CSV export: {e}")
                update.message.reply_text(f"❗ Error creating CSV file: {str(e)}")
                # Clear the conversation state on error
                if user_id in user_states:
                    del user_states[user_id]
        else:
            update.message.reply_text(
                "❗ Invalid choice. Please reply with '1', '2', or a valid CSV file path:\n"
                "1. Yes - I'll provide the file path\n"
                "2. No - Create a new file (default)"
            )
    
    elif state == 'waiting_for_csv_path_input':
        if os.path.isfile(text) and text.lower().endswith('.csv'):
            user_states[user_id]['csv_path'] = text
            process_export_csv(update, context, use_manual_input=True)
        else:
            update.message.reply_text(
                "❗ Invalid file path or file doesn't exist. Please enter a valid CSV file path:"
            )
    
    elif state == 'waiting_for_limit_amount':
        # Try to parse the limit amount
        try:
            # Remove any currency symbols and convert to float
            numeric_str = re.sub(r'[€$£¥]', '', text)
            # Handle both decimal separators
            if ',' in numeric_str and '.' not in numeric_str:
                numeric_str = numeric_str.replace(',', '.')
            
            limit_amount = float(numeric_str)
            selected_bank = user_states[user_id].get('selected_bank')
            
            # Initialize bank limits for this user if not already done
            if user_id not in user_bank_limits:
                user_bank_limits[user_id] = {}
            
            # Set the limit for this bank
            user_bank_limits[user_id][selected_bank] = limit_amount
            
            # Calculate remaining limit
            total_deposit = user_bank_deposits.get(user_id, {}).get(selected_bank, 0)
            remaining_limit = limit_amount - total_deposit
            
            update.message.reply_text(
                f"✅ Limit of {limit_amount} set for {selected_bank}.\n\n"
                f"📊 <b>Remaining Limit Calculation</b>:\n"
                f"Bank Limit: {limit_amount}\n"
                f"Total Deposits: {total_deposit}\n"
                f"<b>Remaining Limit: {remaining_limit}</b>",
                parse_mode='HTML'
            )
            
            # Clear the conversation state
            del user_states[user_id]
        except ValueError:
            update.message.reply_text(
                "❗ Invalid amount. Please enter a valid number for the limit amount:"
            )

def create_socket_lock():
    """Create a socket-based lock to ensure only one instance of the bot runs."""
    lock_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Set socket options to reuse the address and port
        lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Use a specific port for locking - this will fail if another instance is running
        # Use port 10001 to avoid conflicts with the web server
        lock_socket.bind(('localhost', 10001))
        
        # Set a timeout on the socket to prevent indefinite blocking
        lock_socket.settimeout(30)
        
        logging.info("Successfully acquired socket lock - this is the only running instance")
        return lock_socket
    except socket.error as e:
        logging.error(f"Failed to acquire socket lock - another instance is already running: {e}")
        # Try to close the socket if it was created but not bound
        try:
            lock_socket.close()
        except:
            pass
        return None

def main():
    """Start the bot."""
    # Try to acquire the lock to ensure only one instance runs
    lock_socket = create_socket_lock()
    if not lock_socket:
        logging.error("Exiting because another instance is already running")
        return
    
    # Acquire thread lock as well for extra safety
    if not BOT_INSTANCE_LOCK.acquire(blocking=False):
        logging.error("Another instance is already running (thread lock)")
        # Close the socket lock since we couldn't acquire the thread lock
        if lock_socket:
            lock_socket.close()
        return
    
    # Add a small delay to ensure any previous instance has fully released resources
    logging.info("Acquired locks successfully, waiting for resources to be fully available...")
    time.sleep(5)
    
    try:
        # Create the Updater and pass it your bot's token
        updater = Updater(BOT_TOKEN)

        # Get the dispatcher to register handlers
        dp = updater.dispatcher

        # Add command handlers
        dp.add_handler(CommandHandler("start", start))
        dp.add_handler(CommandHandler("help", help_command))
        dp.add_handler(CommandHandler("process", process_command))
        dp.add_handler(CommandHandler("clear", clear_command))
        dp.add_handler(CommandHandler("settings", settings_command))
        dp.add_handler(CommandHandler("export_csv", export_csv))
        dp.add_handler(CommandHandler("export_json", export_json))
        dp.add_handler(CommandHandler("stats", stats_command))

        # Add callback query handler
        dp.add_handler(CallbackQueryHandler(button_callback))
        
        # Add message handler - handle conversation first, then regular message collection
        # This ensures that messages during a conversation flow are handled correctly
        dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_conversation))

        # Add error handler for Conflict errors
        dp.add_error_handler(error_handler)

        # Keep the bot alive by starting the Flask server
        # Continue even if keep_alive fails
        keep_alive_success = keep_alive()
        if not keep_alive_success:
            logging.warning("Keep alive server failed to start, but continuing with bot operation")

        # Start the Bot with a higher allowed_updates interval to prevent conflicts
        max_retries = 5  # Increased from 3 to 5
        retry_count = 0
        backoff_time = 5  # Initial backoff time in seconds
        
        while retry_count < max_retries:
            try:
                # More aggressive cleanup before starting
                logging.info(f"Attempt {retry_count + 1}/{max_retries} to start bot")
                
                # First ensure the webhook is deleted
                updater.bot.delete_webhook(drop_pending_updates=True)
                logging.info("Webhook deleted and pending updates dropped")
                
                # Wait a moment for Telegram servers to process the webhook deletion
                time.sleep(2)
                
                # Start polling with drop_pending_updates to ignore old messages
                updater.start_polling(timeout=30, drop_pending_updates=True)
                logging.info("Bot started successfully")
                break  # Exit the retry loop if successful
            except Conflict as ce:
                retry_count += 1
                logging.error(f"Conflict error on attempt {retry_count}/{max_retries}: {ce}")
                if retry_count >= max_retries:
                    logging.error("Maximum retry attempts reached. Exiting.")
                    return
                # Exponential backoff between retries
                backoff_time *= 1.5
                logging.info(f"Waiting {backoff_time:.1f} seconds before next attempt...")
                time.sleep(backoff_time)
            except Exception as e:
                logging.error(f"Failed to start bot: {e}")
                # If we can't start polling, wait a bit and try again with more aggressive settings
                retry_count += 1
                backoff_time *= 1.5
                logging.info(f"Waiting {backoff_time:.1f} seconds before next attempt...")
                time.sleep(backoff_time)
                if retry_count >= max_retries:
                    logging.error("Maximum retry attempts reached. Exiting.")
                    return

        # Run the bot until you press Ctrl-C
        updater.idle()
    finally:
        # Release the lock when the bot is shutting down
        if BOT_INSTANCE_LOCK.locked():
            BOT_INSTANCE_LOCK.release()
        if lock_socket:
            lock_socket.close()

if __name__ == '__main__':
    main()