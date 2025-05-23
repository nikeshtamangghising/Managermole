import re
import logging
import os
import time
import csv
import json
import socket
import threading
import random
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import Updater, CommandHandler, MessageHandler, CallbackQueryHandler, Filters
from telegram.error import Conflict, TelegramError, NetworkError
import requests

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

# Flag to track if shutdown is in progress
SHUTDOWN_IN_PROGRESS = False

def create_socket_lock():
    """Create a socket-based lock to ensure only one instance of the bot runs."""
    lock_port = 10001
    
    try:
        # Create a new socket for locking
        lock_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Set socket options for better exclusivity
        lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
            lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        
        # Set socket to non-blocking mode
        lock_socket.setblocking(False)
        
        # Try to bind to the lock port with a timeout to handle race conditions
        try:
            lock_socket.bind(('localhost', lock_port))
            logging.info("Successfully acquired socket lock")
            
            # Set up a heartbeat thread to keep the socket alive
            def keep_socket_alive():
                while not getattr(threading.current_thread(), "stop", False):
                    try:
                        # Just keep the thread alive
                        time.sleep(2)
                    except:
                        break
            
            heartbeat_thread = threading.Thread(target=keep_socket_alive, daemon=True)
            heartbeat_thread.start()
            
            return lock_socket
        except socket.error as e:
            logging.error(f"Socket already in use, another instance is likely running: {e}")
            lock_socket.close()
            return None
            
    except Exception as e:
        logging.error(f"Failed to create socket lock: {e}")
        return None

def graceful_shutdown():
    """Perform a graceful shutdown of the bot."""
    global bot_updater, SHUTDOWN_IN_PROGRESS
    
    if SHUTDOWN_IN_PROGRESS:
        return
        
    SHUTDOWN_IN_PROGRESS = True
    logging.info("Starting graceful shutdown...")
    
    try:
        # Stop the updater
        if bot_updater:
            try:
                if hasattr(bot_updater, 'bot'):
                    try:
                        bot_updater.bot.delete_webhook(drop_pending_updates=True)
                    except:
                        pass
                
                bot_updater.stop()
                logging.info("Stopped updater")
            except:
                pass
            
    except Exception as e:
        logging.error(f"Error during shutdown: {e}")
    finally:
        SHUTDOWN_IN_PROGRESS = False
        logging.info("Shutdown complete")

def error_handler(update, context):
    """Handle errors in the dispatcher with immediate termination for conflicts."""
    try:
        if isinstance(context.error, Conflict):
            logging.critical("CRITICAL: Bot conflict detected - another instance is running.")
            logging.critical("Shutting down immediately to avoid further conflicts...")
            
            # Force immediate exit without any cleanup
            # This is necessary in containerized environments
            import os
            os._exit(0)  # Use exit code 0 to prevent container restart loops
            
        elif isinstance(context.error, NetworkError):
            logging.error(f"Network error: {context.error}")
        else:
            update_str = str(update) if update else "None"
            logging.error(f"Error processing update: {update_str}")
            logging.error(f"Error details: {context.error}")
    except Exception as e:
        logging.error(f"Error in error handler: {e}")
        import traceback
        logging.error(f"Error traceback: {traceback.format_exc()}")

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

def show_bank_selection(update: Update, context) -> None:
    """Show bank selection keyboard for CSV export with improved categorization and layout."""
    # Get user ID
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message
    else:
        user_id = update.effective_user.id
        message = update.message
    
    # Create a keyboard with Nepali banks and user's custom banks
    keyboard = []
    
    # Add a header row for better organization
    keyboard.append([InlineKeyboardButton("🏦 SELECT A BANK FOR YOUR DEPOSIT 🏦", callback_data="header_no_action")])
    
    # Add user's previous selections first if they exist
    previous_banks = []
    if user_id in user_states and 'bank_deposits' in user_states[user_id]:
        for deposit in user_states[user_id]['bank_deposits']:
            if deposit['bank'] != 'Previous Balance' and deposit['bank'] not in previous_banks:
                previous_banks.append(deposit['bank'])
    
    if previous_banks:
        keyboard.append([InlineKeyboardButton("✅ RECENTLY USED BANKS", callback_data="header_no_action")])
        for i, bank in enumerate(previous_banks):
            bank_index = NEPAL_BANKS.index(bank) if bank in NEPAL_BANKS else -1
            if bank_index >= 0:
                callback_data = f"select_bank_{bank_index}"
            else:
                # Must be a custom bank
                custom_index = user_custom_banks.get(user_id, []).index(bank) if bank in user_custom_banks.get(user_id, []) else -1
                callback_data = f"select_custom_bank_{custom_index}" if custom_index >= 0 else "enter_different_bank"
            
            keyboard.append([InlineKeyboardButton(f"🔄 {bank}", callback_data=callback_data)])
    
    # Add a separator
    keyboard.append([InlineKeyboardButton("───────── NEPAL BANKS ─────────", callback_data="header_no_action")])

def show_bank_selection_with_done(update: Update, context) -> None:
    """Show bank selection keyboard with a Done button to exit the process."""
    # Get user ID
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message
    else:
        user_id = update.effective_user.id
        message = update.message
    
    # Create a keyboard with Nepali banks and user's custom banks
    keyboard = []
    
    # Add a header row for better organization
    keyboard.append([InlineKeyboardButton("🏦 SELECT A BANK OR CLICK DONE 🏦", callback_data="header_no_action")])
    
    # Add a Done button at the top for easy access
    keyboard.append([InlineKeyboardButton("✅ DONE - FINISH BANK ENTRY", callback_data="done_bank_selection")])
    
    # Add user's previous selections first if they exist
    previous_banks = []
    if user_id in user_states and 'bank_deposits' in user_states[user_id]:
        for deposit in user_states[user_id]['bank_deposits']:
            if deposit['bank'] != 'Previous Balance' and deposit['bank'] not in previous_banks:
                previous_banks.append(deposit['bank'])
    
    if previous_banks:
        keyboard.append([InlineKeyboardButton("✅ RECENTLY USED BANKS", callback_data="header_no_action")])
        for i, bank in enumerate(previous_banks):
            bank_index = NEPAL_BANKS.index(bank) if bank in NEPAL_BANKS else -1
            if bank_index >= 0:
                callback_data = f"select_bank_{bank_index}"
            else:
                # Must be a custom bank
                custom_index = user_custom_banks.get(user_id, []).index(bank) if bank in user_custom_banks.get(user_id, []) else -1
                callback_data = f"select_custom_bank_{custom_index}" if custom_index >= 0 else "enter_different_bank"
            
            keyboard.append([InlineKeyboardButton(f"🔄 {bank}", callback_data=callback_data)])
    
    # Add a separator
    keyboard.append([InlineKeyboardButton("───────── NEPAL BANKS ─────────", callback_data="header_no_action")])
    
    # Add default Nepali banks in a more organized way (3 per row)
    row = []
    for i, bank in enumerate(NEPAL_BANKS):
        if i % 3 == 0 and i > 0:
            keyboard.append(row)
            row = []
        row.append(InlineKeyboardButton(bank, callback_data=f"select_bank_{i}"))
    
    if row:  # Add any remaining buttons
        keyboard.append(row)
    
    # Add user's custom banks if any
    if user_id in user_custom_banks and user_custom_banks[user_id]:
        # Add a separator row
        keyboard.append([InlineKeyboardButton("───────── YOUR CUSTOM BANKS ─────────", callback_data="custom_bank_header")])
        
        # Add custom banks (3 per row)
        row = []
        for i, bank in enumerate(user_custom_banks[user_id]):
            if i % 3 == 0 and i > 0:
                keyboard.append(row)
                row = []
            # Use a different prefix for custom banks to distinguish them
            row.append(InlineKeyboardButton(f"🔶 {bank}", callback_data=f"select_custom_bank_{i}"))
        
        if row:  # Add any remaining buttons
            keyboard.append(row)
    
    # Add option to enter a different bank
    keyboard.append([InlineKeyboardButton("Enter Different Bank", callback_data="enter_different_bank")])
    
    # Add the Done button at the bottom as well for convenience
    keyboard.append([InlineKeyboardButton("✅ DONE - FINISH BANK ENTRY", callback_data="done_bank_selection")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Get summary of deposits so far
    deposits_text = ""
    if 'bank_deposits' in user_states[user_id] and user_states[user_id]['bank_deposits']:
        deposits = user_states[user_id]['bank_deposits']
        deposits_summary = "\n".join([f"• <b>{d['bank']}</b>: {d['amount']:.2f}" for d in deposits])
        deposits_text = f"\n\n<b>Current deposits:</b>\n{deposits_summary}\n\n<b>Total:</b> {user_states[user_id].get('total_deposits', 0):.2f}"
    
    # Send the message with the keyboard
    message_text = f"🏦 <b>Please select a bank or click Done when finished:</b>{deposits_text}"
    
    # Use the appropriate method based on the update type
    if hasattr(update, 'message'):
        update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    else:
        # This is for handling cases where we need to send a new message after a callback query
        context.bot.send_message(chat_id=user_id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
    for i, bank in enumerate(NEPAL_BANKS):
        if i % 3 == 0 and i > 0:
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
        keyboard.append([InlineKeyboardButton("───────── YOUR CUSTOM BANKS ─────────", callback_data="custom_bank_header")])
        
        # Add custom banks (3 per row)
        for i, bank in enumerate(user_custom_banks[user_id]):
            if i % 3 == 0 and i > 0:
                keyboard.append(row)
                row = []
            # Use a different prefix for custom banks to distinguish them
            row.append(InlineKeyboardButton(f"🔶 {bank}", callback_data=f"select_custom_bank_{i}"))
    
    # Add option to enter a different bank name
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("───────── OTHER OPTIONS ─────────", callback_data="header_no_action")])
    keyboard.append([InlineKeyboardButton("✏️ Enter a different bank name", callback_data="enter_different_bank")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # If this is from a callback query, use edit_message_text
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        update.callback_query.edit_message_text(
            text="🏦 <b>Please select a bank for your deposit:</b>\n\nChoose from the list below or enter a custom bank name.",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    else:
        # Otherwise, send a new message
        message.reply_text(
            "🏦 <b>Please select a bank for your deposit:</b>\n\nChoose from the list below or enter a custom bank name.",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )

def ask_for_deposit_info(update: Update, context) -> None:
    """Ask the user for deposit amount, bank name, and remaining balance.
    Supports multiple bank deposits for the same day and improved balance tracking."""
    user_id = update.effective_user.id
    
    # Initialize user state for CSV export with enhanced structure
    user_states[user_id] = {
        'state': 'waiting_for_remaining_balance',
        'action': 'csv_export',
        'remaining_balance': None,  # Will store the manually entered remaining balance
        'bank_deposits': [],  # Will store multiple bank deposits for the same day
        'current_bank': None,  # Will store the currently selected bank
        'csv_path': None,  # Will store the CSV file path if appending to existing file
        'total_deposits': 0.0,  # Will track the running total of deposits
        'total_paid': 0.0  # Will track the running total of payments
    }
    
    # First ask for remaining balance with improved instructions
    message_text = (
        "💰 <b>Please enter your remaining balance first:</b>\n\n"
        "This will be used as the starting balance for your report and included in calculations. "
        "If you're continuing from a previous report, this should be your current balance.\n\n"
        "Enter 0 if you don't want to include a remaining balance."
    )
    
    # If this is from a callback query, use edit_message_text
    if hasattr(update, 'callback_query'):
        update.callback_query.edit_message_text(text=message_text, parse_mode='HTML')
    else:
        # Otherwise, send a new message
        update.message.reply_text(message_text, parse_mode='HTML')


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
        
        # Set the current bank and transition to deposit amount entry
        user_states[user_id]['current_bank'] = bank_name
        user_states[user_id]['state'] = 'waiting_for_deposit_amount'
        
        update.message.reply_text(
            f"✅ Custom bank '{bank_name}' has been added.\n\n"
            f"Please enter the deposit amount for {bank_name}:"
        )
        return

    elif state == 'waiting_for_bank_name':
        # User is entering a custom bank name for this transaction
        bank_name = text.strip()
        
        # Check if bank already exists in default list
        if bank_name in NEPAL_BANKS:
            update.message.reply_text(
                f"❗ '{bank_name}' already exists in the default bank list. Please enter a different name:"
            )
            return
        
        # Set the current bank and transition to deposit amount entry
        user_states[user_id]['current_bank'] = bank_name
        user_states[user_id]['state'] = 'waiting_for_deposit_amount'
        
        update.message.reply_text(
            f"✅ Bank name '{bank_name}' has been set.\n\n"
            f"Please enter the deposit amount for {bank_name}:"
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
            
            # Get the current bank
            current_bank = user_states[user_id]['current_bank']
            
            # Initialize bank_deposits list if it doesn't exist
            if 'bank_deposits' not in user_states[user_id]:
                user_states[user_id]['bank_deposits'] = []
            
            # Add to bank deposits list
            user_states[user_id]['bank_deposits'].append({
                'bank': current_bank,
                'amount': deposit_amount
            })
            
            # Update running total of deposits
            user_states[user_id]['total_deposits'] += deposit_amount
            
            # Calculate current balance
            current_balance = user_states[user_id]['total_deposits'] - user_states[user_id].get('total_paid', 0)
            
            # Ask if user wants to add another bank deposit
            keyboard = [
                [InlineKeyboardButton("Add Another Bank Deposit", callback_data='add_another_bank')],
                [InlineKeyboardButton("Finish and Export CSV", callback_data='finish_csv_export')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Show summary of current deposits with improved formatting
            deposits_summary = "\n".join([f"• <b>{d['bank']}</b>: {d['amount']:.2f}" for d in user_states[user_id]['bank_deposits']])
            
            update.message.reply_text(
                f"✅ <b>Added deposit of {deposit_amount:.2f} to {current_bank}</b>\n\n"
                f"<b>Current deposits:</b>\n{deposits_summary}\n\n"
                f"<b>Running total:</b> {user_states[user_id]['total_deposits']:.2f}\n"
                f"<b>Current balance:</b> {current_balance:.2f}\n\n"
                f"<b>Would you like to add another bank deposit or finish?</b>",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except ValueError:
            update.message.reply_text(
                "❗ Invalid amount. Please enter a valid number for the deposit amount:"
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
            
            # Add the remaining balance as a special entry if it's greater than 0
            if remaining_balance > 0:
                user_states[user_id]['bank_deposits'] = [{
                    'bank': 'Previous Balance',
                    'amount': remaining_balance
                }]
                user_states[user_id]['total_deposits'] = remaining_balance
            
            # Show bank selection for deposit entry
            show_bank_selection_with_done(update, context)
            
        except ValueError:
            update.message.reply_text(
                "❗ Invalid number format. Please enter a valid number for the remaining balance:"
            )
    
    elif state == 'waiting_for_csv_path':
        if text == '1':
            user_states[user_id]['state'] = 'waiting_for_csv_path_input'
            update.message.reply_text(
                "<b>Provide existing CSV file path</b>\n\n"
                "📝 Please enter the full path to your CSV file (e.g., C:\\Users\\YourName\\Documents\\my_file.csv):",
                parse_mode='HTML'
            )
        elif text == '2' or text.lower() in ['no', 'default', 'new']:
            # Use default filename (no CSV path)
            user_states[user_id]['csv_path'] = None
            update.message.reply_text(
                "<b>Creating new CSV file</b>\n\n"
                "📊 Creating a new CSV file with your deposit information...",
                parse_mode='HTML'
            )
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
            update.message.reply_text(
                f"<b>Appending to existing CSV file</b>\n\n"
                f"📊 Appending to your existing CSV file at:\n{text}",
                parse_mode='HTML'
            )
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
            update.message.reply_text(
                f"<b>Appending to existing CSV file</b>\n\n"
                f"📊 Appending to your existing CSV file at:\n{text}",
                parse_mode='HTML'
            )
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
    """Export the results as a simple CSV file with amounts, charges, row sums, and running totals in a clearer format."""
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
            
            # Write header with four columns: Amount, Charge, Row Sum, Running Total
            writer.writerow(['Amount', 'Charge', 'Row Sum', 'Running Total'])
            
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
                
                # Calculate row sum (amount + charge)
                row_sum = amount_numeric + charge_numeric
                
                # Update running total (add the row sum to the running total)
                running_total += row_sum
                
                # Write the row with row sum and running total
                writer.writerow([amount_value, charge_value, f"{row_sum:.2f}", f"{running_total:.2f}"])
            
            # Write total row
            writer.writerow(['', '', '', ''])
            writer.writerow(['TOTAL', '', '', f"{running_total:.2f}"])

        # Send the file to the user
        with open(filename, 'rb') as file:
            message.reply_document(
                document=file,
                filename=os.path.basename(filename),
                caption=f"📊 Simple CSV export with improved format.\n\nThe file includes:\n- Amounts in the first column\n- Charges in the second column\n- Row Sum in the third column (adds amount and charge for each row)\n- Running total in the fourth column (cumulative sum of all row sums)\n- Final total at the bottom"
            )

        # Remove the temporary file
        os.remove(filename)

    except Exception as e:
        logger.error(f"Error exporting simple CSV: {e}")
        message.reply_text(
            f"❗ Sorry, there was an error creating your CSV file: {str(e)}"
        )

def process_export_csv(update: Update, context, use_manual_input=False) -> None:
    """Export the results as a CSV file with the format: Date, Deposit Amount, Bank Name, Paid To Host, Total Deposit, Total Paid, Remaining Balance.
    Maintains a running balance by using the previous day's remaining balance as today's starting balance.
    Supports multiple bank deposits on the same day and provides detailed summaries."""
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
    
    # Initialize bank deposits list
    bank_deposits = []
    
    # Use manual input if requested
    if use_manual_input and user_id in user_states:
        # Get the manually entered remaining balance
        manual_remaining_balance = user_states[user_id].get('remaining_balance')
        csv_path = user_states[user_id].get('csv_path')
        
        # Get multiple bank deposits if available
        if 'bank_deposits' in user_states[user_id] and user_states[user_id]['bank_deposits']:
            bank_deposits = user_states[user_id]['bank_deposits']
            
            # We'll handle bank deposits separately - don't add to amounts list
            # for deposit in bank_deposits:
            #     deposit_amount = deposit['amount']
            #     # Convert to string with appropriate format
            #     deposit_str = str(int(deposit_amount) if deposit_amount.is_integer() else deposit_amount)
            #     amounts.append(deposit_str)
        else:
            # Fallback to old single deposit method if no bank_deposits list
            deposit_amount = user_states[user_id].get('deposit_amount')
            bank_name = user_states[user_id].get('bank_name')
            
            if deposit_amount is not None and bank_name is not None:
                # Don't add to amounts list, handle separately
                # deposit_str = str(int(deposit_amount) if deposit_amount.is_integer() else deposit_amount)
                # amounts.append(deposit_str)
                bank_deposits.append({
                    'bank': bank_name,
                    'amount': deposit_amount
                })
    else:
        csv_path = None
        manual_remaining_balance = None
        
    # Variable to store previous day's balance
    previous_balance = 0.0
    
    # If user manually entered a remaining balance, use it instead of reading from file
    if manual_remaining_balance is not None:
        previous_balance = manual_remaining_balance
        logger.info(f"Using manually entered remaining balance: {previous_balance}")
    
    # Process all collected messages to extract both amounts and charges
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

    # Convert amounts to numeric values
    amounts_numeric = []
    for amount in amounts:
        try:
            # Remove any currency symbol and convert to float
            numeric_str = re.sub(r'[€$£¥]', '', amount)
            # Handle both decimal separators
            if decimal_separator == ',':
                numeric_str = numeric_str.replace(',', '.')
            amounts_numeric.append(float(numeric_str))
        except ValueError:
            amounts_numeric.append(0.0)
    
    # Convert charges to numeric values
    charges_numeric = []
    for charge in charges:
        try:
            # Remove any currency symbol and convert to float
            numeric_str = re.sub(r'[€$£¥]', '', charge)
            # Handle both decimal separators
            if decimal_separator == ',':
                numeric_str = numeric_str.replace(',', '.')
            charges_numeric.append(float(numeric_str))
        except ValueError:
            charges_numeric.append(0.0)

    # Calculate deposit total (including bank deposits)
    total_deposit = sum(amounts_numeric)
    
    # Add bank deposits to total
    for deposit in bank_deposits:
        if deposit['bank'] != 'Previous Balance':  # Don't double count previous balance
            total_deposit += deposit['amount']
    
    # If we have a previous balance, add it to the total deposit
    if previous_balance > 0:
        total_deposit += previous_balance

    # Determine the CSV file path
    if csv_path and os.path.isfile(csv_path):
        filename = csv_path
        file_exists = True
        # Store the path for future use
        user_csv_files[user_id] = csv_path
    else:
        # Create a new CSV file with the requested format using current month
        current_dir = os.path.dirname(os.path.abspath(__file__))
        current_month = datetime.now().strftime('%B_%Y')  # e.g., "March_2024"
        filename = os.path.join(current_dir, f"decimal_stripper_export_{current_month}.csv")
        file_exists = False
        # Store the path for future use
        user_csv_files[user_id] = filename

    try:
        if file_exists:
            # Read existing file to get current totals
            existing_totals = {'total_deposit': 0, 'total_paid': 0, 'balance': 0}
            try:
                with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    rows = list(reader)
                    
                    # Check if file has the expected format
                    if len(rows) > 0 and 'Date' in rows[0] and 'Total Deposit' in rows[0]:
                        # Find the totals row
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
                file_exists = False
            
            # Open file in append mode
            mode = 'a'
            
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
            
            # If we have manually entered bank deposits, write each one
            if bank_deposits:
                # Sort deposits to ensure Previous Balance comes first if it exists
                sorted_deposits = sorted(bank_deposits, key=lambda x: 0 if x['bank'] == 'Previous Balance' else 1)
                
                for i, deposit in enumerate(sorted_deposits):
                    bank_name = deposit['bank']
                    deposit_amount = deposit['amount']
                    
                    # Format the deposit amount with two decimal places
                    deposit_amount_formatted = f"{float(deposit_amount):.2f}"
                    
                    # Write the deposit information
                    # Only include date in the first row
                    if i == 0:
                        deposit_row = [current_date, deposit_amount_formatted, bank_name, '', '', '', '']
                    else:
                        deposit_row = ['', deposit_amount_formatted, bank_name, '', '', '', '']
                    writer.writerow(deposit_row)
            else:
                # No manually entered deposits, use the first amount from extracted data
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
                deposit_row = [current_date, deposit_amount_formatted, "Remaining Balance", '', '', '', '']
                writer.writerow(deposit_row)
            
            # Calculate each amount + charge pair and show in Paid To Host column
            paid_to_host_sum = 0.0
            
            # Make sure we have the minimum number of rows we need
            min_rows = min(len(amounts_numeric), len(charges_numeric))
            
            # For each pair, add amount and charge
            for i in range(min_rows):
                # Get the values for this row
                amount_val = amounts_numeric[i]
                charge_val = charges_numeric[i]
                
                # Add them together
                row_sum = amount_val + charge_val
                
                # Add to running total
                paid_to_host_sum += row_sum
                
                # Write the sum to Paid To Host column
                row = ['', '', '', f"{row_sum:.2f}", '', '', '']
                writer.writerow(row)
            
            # Add empty row before totals
            writer.writerow(['', '', '', '', '', '', ''])
            
            # Format the totals with two decimal places
            total_deposit_formatted = f"{total_deposit:.2f}"
            # IMPORTANT: total_paid should ONLY be the sum of the Paid To Host columns
            total_paid_formatted = f"{paid_to_host_sum:.2f}"
            # Balance is deposits minus what was paid to host
            balance_formatted = f"{total_deposit - paid_to_host_sum:.2f}"
            
            # Write the totals row
            totals_row = ['', '', '', '', total_deposit_formatted, total_paid_formatted, balance_formatted]
            writer.writerow(totals_row)

        # Send the file to the user
        with open(filename, 'rb') as file:
            message.reply_document(
                document=file,
                filename=os.path.basename(filename),
                caption=f"📊 CSV export with deposit, paid to host (amount+charge), and balance."
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
    
    # Handle header buttons that should not trigger any action
    if data == "header_no_action" or data == "custom_bank_header":
        query.answer("This is just a header, please select an option below.")
        return

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
        # User wants the detailed CSV export - Step 1: Ask for remaining balance
        # Initialize user state for CSV export with enhanced structure
        user_states[user_id] = {
            'state': 'waiting_for_remaining_balance',
            'action': 'csv_export',
            'remaining_balance': None,  # Will store the manually entered remaining balance
            'bank_deposits': [],  # Will store multiple bank deposits for the same day
            'current_bank': None,  # Will store the currently selected bank
            'csv_path': None,  # Will store the CSV file path if appending to existing file
            'total_deposits': 0.0,  # Will track the running total of deposits
            'total_paid': 0.0  # Will track the running total of payments
        }
        
        # Step 1: Ask for remaining balance with improved instructions
        message_text = (
            "💰 <b>Step 1: Please enter your remaining balance:</b>\n\n"
            "This will be used as the starting balance for your report and included in calculations. "
            "If you're continuing from a previous report, this should be your current balance.\n\n"
            "Enter 0 if you don't want to include a remaining balance."
        )
        
        query.edit_message_text(text=message_text, parse_mode='HTML')
        return
    elif data == 'csv_manual_input':
        # Step 3: Show bank selection for deposit entry
        query.edit_message_text(
            "<b>Step 3: Please select a bank for your deposit:</b>\n\n"
            "Choose a bank from the list below. After selecting a bank, you'll be asked to enter the deposit amount.\n\n"
            "You can select multiple banks one by one. When you're done adding all banks, click 'Finish and Export CSV'.",
            parse_mode='HTML'
        )
        # Show bank selection with a Done button
        show_bank_selection_with_done(update, context)
        return
    elif data == 'csv_auto_export':
        # User wants to use only extracted data
        query.edit_message_text(text="Processing CSV export with extracted data...")
        process_export_csv(update, context, use_manual_input=False)
        return
    elif data == 'add_another_bank':
        # Step 5: User wants to add another bank deposit
        query.edit_message_text(
            "<b>Step 5: Add another bank deposit</b>\n\n"
            "You can select another bank to add more deposits, or click 'Finish and Export CSV' when you've finished adding all your bank deposits.",
            parse_mode='HTML'
        )
        show_bank_selection_with_done(update, context)
        return
    elif data == 'finish_csv_export':
        # Step 6: User wants to finish and export CSV - Ask about file creation/append
        query.edit_message_text(
            "<b>Step 6: Choose file option</b>\n\n"
            "📝 Do you want to append to an existing CSV file or create a new one?\n\n"
            "1. Yes - I'll provide the file path to append to\n"
            "2. No - Create a new file (default)\n\n"
            "Please reply with '1' or '2', or enter the full path to your CSV file.\n\n"
            "<b>Note:</b> This will be the final step before generating your well-managed CSV file with all your bank deposits and transaction details.",
            parse_mode='HTML'
        )
        
        # Update state
        user_states[user_id]['state'] = 'waiting_for_csv_path'
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
    elif data == 'done_bank_selection':
        # User is done with bank selection
        if user_id in user_states and user_states[user_id].get('action') == 'deposit_entry':
            # Check if any deposits were made
            if 'bank_deposits' in user_states[user_id] and user_states[user_id]['bank_deposits']:
                # Show summary of deposits
                deposits_summary = "\n".join([f"• <b>{d['bank']}</b>: {d['amount']:.2f}" for d in user_states[user_id]['bank_deposits']])
                total_deposits = user_states[user_id]['total_deposits']
                
                query.edit_message_text(
                    f"✅ <b>Bank deposit entry completed</b>\n\n"
                    f"<b>Deposits recorded:</b>\n{deposits_summary}\n\n"
                    f"<b>Total deposits:</b> {total_deposits:.2f}\n\n"
                    f"Thank you for using the bank deposit entry feature!",
                    parse_mode='HTML'
                )
            else:
                # No deposits were made
                query.edit_message_text(
                    "❗ No deposits were recorded.\n\n"
                    "You can start again using the /menu command."
                )
            
            # Clear the user state
            del user_states[user_id]
        else:
            query.edit_message_text(
                "Operation cancelled. Use /menu to access other features."
            )
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
            user_states[user_id]['current_bank'] = selected_bank
            user_states[user_id]['state'] = 'waiting_for_deposit_amount'
        elif user_states[user_id].get('action') == 'limit_check':
            query.edit_message_text(text=f"Selected bank: {selected_bank}\n\nPlease enter the limit amount for this bank:")
            user_states[user_id]['state'] = 'waiting_for_limit_amount'
        elif user_states[user_id].get('action') == 'csv_export':
            # Step 4: For CSV export, store the bank name and ask for deposit amount
            user_states[user_id]['current_bank'] = selected_bank
            query.edit_message_text(
                f"<b>Step 4: Enter deposit amount for {selected_bank}</b>\n\n"
                f"Please enter the deposit amount for this bank:",
                parse_mode='HTML'
            )
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

def show_bank_selection_with_done(update: Update, context) -> None:
    """Show a keyboard with bank selection options and a Done button."""
    # Determine if this is called from a callback query or directly
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message
    else:
        user_id = update.effective_user.id
        message = update.message
    
    # Create a keyboard with Nepali banks and user's custom banks
    keyboard = []
    
    # Add a header row for better organization
    keyboard.append([InlineKeyboardButton("🏦 SELECT A BANK FOR YOUR DEPOSIT 🏦", callback_data="header_no_action")])
    
    # Add user's previous selections first if they exist (for CSV export)
    previous_banks = []
    if user_id in user_states and 'bank_deposits' in user_states[user_id]:
        for deposit in user_states[user_id]['bank_deposits']:
            if deposit['bank'] != 'Previous Balance' and deposit['bank'] not in previous_banks:
                previous_banks.append(deposit['bank'])
    
    if previous_banks:
        keyboard.append([InlineKeyboardButton("✅ RECENTLY USED BANKS", callback_data="header_no_action")])
        for bank in previous_banks:
            bank_index = NEPAL_BANKS.index(bank) if bank in NEPAL_BANKS else -1
            if bank_index >= 0:
                callback_data = f"select_bank_{bank_index}"
            else:
                # Must be a custom bank
                custom_index = user_custom_banks.get(user_id, []).index(bank) if bank in user_custom_banks.get(user_id, []) else -1
                callback_data = f"select_custom_bank_{custom_index}" if custom_index >= 0 else "enter_different_bank"
            
            keyboard.append([InlineKeyboardButton(f"🔄 {bank}", callback_data=callback_data)])
    
    # Add a separator
    keyboard.append([InlineKeyboardButton("───────── NEPAL BANKS ─────────", callback_data="header_no_action")])
    
    # Add default Nepali banks in a more organized way (3 per row)
    for i in range(0, len(NEPAL_BANKS), 3):
        row = []
        for j in range(3):
            if i + j < len(NEPAL_BANKS):
                bank = NEPAL_BANKS[i + j]
                row.append(InlineKeyboardButton(bank, callback_data=f"select_bank_{i + j}"))
        keyboard.append(row)
    
    # Add user's custom banks if any
    if user_id in user_custom_banks and user_custom_banks[user_id]:
        # Add a header for custom banks
        keyboard.append([InlineKeyboardButton("───────── YOUR CUSTOM BANKS ─────────", callback_data="custom_bank_header")])
        
        # Add custom banks (3 per row)
        for i in range(0, len(user_custom_banks[user_id]), 3):
            row = []
            for j in range(3):
                if i + j < len(user_custom_banks[user_id]):
                    bank = user_custom_banks[user_id][i + j]
                    # Use a different prefix for custom banks to distinguish them
                    row.append(InlineKeyboardButton(f"🔶 {bank}", callback_data=f"select_custom_bank_{i + j}"))
            keyboard.append(row)
    
    # Add option to enter a different bank
    keyboard.append([InlineKeyboardButton("Enter Different Bank", callback_data="enter_different_bank")])
    
    # Get summary of deposits so far
    deposits_text = ""
    if user_id in user_states and 'bank_deposits' in user_states[user_id] and user_states[user_id]['bank_deposits']:
        deposits = user_states[user_id]['bank_deposits']
        deposits_summary = "\n".join([f"• <b>{d['bank']}</b>: {d['amount']:.2f}" for d in deposits])
        deposits_text = f"\n\n<b>Current deposits:</b>\n{deposits_summary}\n\n<b>Total:</b> {user_states[user_id].get('total_deposits', 0):.2f}"
    
    # Add a Done button to exit the bank selection process
    keyboard.append([InlineKeyboardButton("✅ Finish and Export CSV", callback_data="finish_csv_export")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # If this is from a callback query, use edit_message_text
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        update.callback_query.edit_message_text(
            f"<b>Step 3: Select a bank for your deposit</b>\n\n"
            f"Choose a bank from the list below. After selecting a bank, you'll be asked to enter the deposit amount.{deposits_text}\n\n"
            f"When you've finished adding all your bank deposits, click 'Finish and Export CSV'.",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    else:
        # Otherwise, send a new message
        message.reply_text(
            f"<b>Step 3: Select a bank for your deposit</b>\n\n"
            f"Choose a bank from the list below. After selecting a bank, you'll be asked to enter the deposit amount.{deposits_text}\n\n"
            f"When you've finished adding all your bank deposits, click 'Finish and Export CSV'.",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )

def start_bank_deposit_entry(update: Update, context) -> None:
    """Start the process of entering a bank deposit manually."""
    user_id = update.callback_query.from_user.id
    
    # Initialize user state
    user_states[user_id] = {
        'state': 'selecting_bank',
        'action': 'deposit_entry',
        'bank_deposits': [],  # Initialize bank_deposits list
        'total_deposits': 0.0  # Initialize total_deposits
    }
    
    # Create a keyboard with Nepali banks and user's custom banks
    keyboard = []
    row = []
    
    # Add default Nepali banks in a more organized way (3 per row)
    for i, bank in enumerate(NEPAL_BANKS):
        if i % 3 == 0 and i > 0:
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
        keyboard.append([InlineKeyboardButton("───────── YOUR CUSTOM BANKS ─────────", callback_data="custom_bank_header")])
        
        # Add custom banks (3 per row)
        for i, bank in enumerate(user_custom_banks[user_id]):
            if i % 3 == 0 and i > 0:
                keyboard.append(row)
                row = []
            # Use a different prefix for custom banks to distinguish them
            row.append(InlineKeyboardButton(f"🔶 {bank}", callback_data=f"select_custom_bank_{i}"))
    
    if row:  # Add any remaining buttons
        keyboard.append(row)
    
    # Add a Done button to exit the bank selection process
    keyboard.append([InlineKeyboardButton("✅ Done", callback_data="done_bank_selection")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Ask for remaining balance first if not already set
    if 'remaining_balance' not in user_states[user_id]:
        update.callback_query.edit_message_text(
            "💰 <b>Please enter your remaining balance first:</b>\n\n"
            "This will be used as your starting balance.\n\n"
            "Enter 0 if you don't want to include a remaining balance.",
            parse_mode='HTML'
        )
        user_states[user_id]['state'] = 'waiting_for_remaining_balance'
    else:
        update.callback_query.edit_message_text(
            "🏦 Please select a bank or click Done when finished:",
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
    
    # Add default Nepali banks in a more organized way (3 per row)
    for i, bank in enumerate(NEPAL_BANKS):
        if i % 3 == 0 and i > 0:
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
        keyboard.append([InlineKeyboardButton("───────── YOUR CUSTOM BANKS ─────────", callback_data="custom_bank_header")])
        
        # Add custom banks (3 per row)
        for i, bank in enumerate(user_custom_banks[user_id]):
            if i % 3 == 0 and i > 0:
                keyboard.append(row)
                row = []
            # Use a different prefix for custom banks to distinguish them
            row.append(InlineKeyboardButton(f"🔶 {bank}", callback_data=f"select_custom_bank_{i}"))
    
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

def check_bot_already_running(bot_token):
    """Check directly with the Telegram API if this bot is already running elsewhere."""
    import requests
    import time
    import random
    
    # Generate a unique test message to identify this instance
    instance_id = f"instance_check_{random.randint(1000000, 9999999)}_{int(time.time())}"
    
    # First, try to get updates to see if another instance is polling
    try:
        # Direct API call to check for getUpdates conflicts
        api_url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        params = {
            "timeout": 1,
            "offset": -1,
            "limit": 1
        }
        
        # First attempt - if this succeeds without Conflict error, no other instance is running
        response = requests.post(api_url, json=params, timeout=5)
        response_json = response.json()
        
        # Check if we got a conflict error
        if not response_json.get('ok', False) and "conflict" in response_json.get('description', '').lower():
            logging.error(f"Bot already running (getUpdates conflict): {response_json.get('description')}")
            return True
            
        # No conflict detected through getUpdates
        logging.info("No bot instance detected via getUpdates")
        return False
        
    except Exception as e:
        # If there was an error checking, better to assume no conflict and proceed
        logging.warning(f"Error checking if bot is running: {e}")
        return False


def initialize_bot_safely():
    """Initialize the bot with comprehensive conflict prevention."""
    global bot_updater
    
    # Check if another instance is already running
    if check_bot_already_running(BOT_TOKEN):
        logging.error("Another bot instance is already running based on API check. Exiting.")
        return None
    
    try:
        logging.info("Initializing bot with conflict prevention...")
        
        # Aggressive cleanup of Telegram webhook and pending updates
        cleanup_bot = Bot(BOT_TOKEN)
        
        # First delete webhook and drop all pending updates
        cleanup_bot.delete_webhook(drop_pending_updates=True)
        logging.info("Deleted webhook and dropped pending updates")
        time.sleep(2)
        
        # Then force a getUpdates call with short timeout to reset the API state
        try:
            cleanup_bot.get_updates(offset=-1, limit=1, timeout=1)
            logging.info("Reset API state with get_updates call")
        except Exception as update_error:
            logging.info(f"Expected error during get_updates reset: {update_error}")
        
        time.sleep(2)
        
        # Final webhook deletion to be sure
        cleanup_bot.delete_webhook(drop_pending_updates=True)
        logging.info("Final webhook cleanup complete")
        
        # Wait to ensure cleanup is complete
        time.sleep(3)
        
        # Create updater with proper error handling
        updater = Updater(
            BOT_TOKEN, 
            request_kwargs={
                'read_timeout': 30,
                'connect_timeout': 30
            },
            use_context=True
        )
        
        return updater
    except Exception as e:
        logging.error(f"Error initializing bot: {e}")
        return None


def main():
    """Start the bot with comprehensive conflict prevention and recovery."""
    global bot_updater
    
    # Special handling for Render.com environment
    import os  # Make sure os is imported in this scope
    is_render = os.environ.get('RENDER', '') == 'true'
    if is_render:
        logging.info("Detected Render.com environment - using specialized setup")
        # Ensure we're using the correct port
        port = os.environ.get('PORT')
        logging.info(f"Render assigned PORT: {port}")
        
        # Make sure the PORT is set in environment variables
        if port:
            # Port must be available in this process and in child threads
            os.environ['PORT'] = port
            logging.info(f"Set PORT environment variable to {port}")
        else:
            # If no port is assigned, set a default for Render that works with web services
            os.environ['PORT'] = '10000'  # Render expects a port to be open
            logging.warning("No PORT specified in Render environment, using default: 10000")
        
        # Add additional information to logs
        render_instance = os.environ.get('RENDER_INSTANCE_ID', 'unknown')
        render_service = os.environ.get('RENDER_SERVICE_NAME', 'unknown')
        logging.info(f"Running as Render service: {render_service}, instance: {render_instance}")
    
    # Max number of restart attempts
    max_restarts = 5
    restart_attempts = 0
    
    while restart_attempts < max_restarts:
        try:
            # Initialize the bot with conflict prevention
            updater = initialize_bot_safely()
            if not updater:
                logging.error("Failed to initialize bot safely. Waiting to retry...")
                time.sleep(30)  # Wait before retry
                restart_attempts += 1
                continue
            
            bot_updater = updater
            
            # Register handlers
            dp = updater.dispatcher
            dp.add_handler(CommandHandler("start", start))
            dp.add_handler(CommandHandler("help", help_command))
            dp.add_handler(CommandHandler("process", process_command))
            dp.add_handler(CommandHandler("clear", clear_command))
            dp.add_handler(CommandHandler("settings", settings_command))
            dp.add_handler(CommandHandler("export_csv", export_csv))
            dp.add_handler(CommandHandler("export_json", export_json))
            dp.add_handler(CommandHandler("stats", stats_command))
            dp.add_handler(CallbackQueryHandler(button_callback))
            dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_conversation))
            dp.add_error_handler(error_handler)

            # Start keep-alive server - MUST be done before bot polling on Render
            keep_alive_success = keep_alive()
            if is_render and not keep_alive_success:
                logging.critical("Failed to start keep-alive server on Render. Service might fail!")
            
            # Start the bot with conflict handling and automatic recovery
            try:
                logging.info("Starting bot polling...")
                updater.start_polling(
                    timeout=30,
                    drop_pending_updates=True,
                    allowed_updates=['message', 'callback_query', 'chat_member']
                )
                logging.info("Bot started successfully")
                
                # Use a different approach to monitor the updater's state
                # Instead of checking an attribute that doesn't exist, we'll 
                # use the idle() method which will block until we receive a stop signal
                updater.idle()
                
                # If idle() returns, it means the bot was stopped externally
                logging.warning("Updater stopped running. Attempting to restart...")
                    
            except Conflict as e:
                logging.critical(f"Conflict detected during polling: {e}")
                # Force exit on conflict
                import os
                os._exit(1)
            except (NetworkError, ConnectionError, requests.RequestException) as e:
                logging.error(f"Network error: {e}. Restarting...")
                time.sleep(30)  # Wait before restart
                restart_attempts += 1
                continue
            except Exception as e:
                logging.error(f"Error during bot polling: {e}")
                time.sleep(30)  # Wait before restart
                restart_attempts += 1
                continue
                
        except Exception as e:
            logging.error(f"Error in main function: {e}")
            import traceback
            logging.error(traceback.format_exc())
            time.sleep(30)  # Wait before restart
            restart_attempts += 1
        finally:
            if updater:
                try:
                    updater.stop()
                    logging.info("Updater stopped")
                except:
                    pass
    
    # If we've reached max restarts, log a critical error
    if restart_attempts >= max_restarts:
        logging.critical(f"Reached maximum restart attempts ({max_restarts}). Please check the bot configuration.")
    
    logging.info("Bot shutdown complete")


# Global variables to track bot state
bot_updater = None

# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    """Handle termination signals to ensure graceful shutdown."""
    logging.info(f"Received signal {sig}, initiating graceful shutdown...")
    graceful_shutdown()
    import os
    os._exit(0)  # Force exit to avoid restart loops

if __name__ == "__main__":
    try:
        # Register signal handlers for graceful shutdown
        import signal
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the bot - no process checks needed as we now use API-based checks
        logging.info("Starting bot...")
        main()
    except Exception as e:
        logging.critical(f"Critical error during startup: {e}")
        import traceback
        logging.critical(traceback.format_exc())