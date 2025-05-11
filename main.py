
import re
import logging
import os
import time
import csv
import json
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, MessageHandler, CallbackQueryHandler, Filters

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
        "/settings - Customize your number processing preferences\n"
        "/stats - View statistics about your collected messages\n\n"

        "📊 <b>Export Options</b>:\n"
        "/export_csv - Export results as a CSV file with row-by-row sums\n"
        "/export_json - Export results as a JSON file\n\n"

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

def export_csv(update: Update, context) -> None:
    """Export the results as a CSV file with 3 columns: amounts, charges, and row-by-row sums."""
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

    # Create a CSV file with 3 columns
    filename = f"decimal_stripper_export_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Create writer with just 3 columns
            fieldnames = ['Amounts', 'Charges', 'Row Sum']
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(fieldnames)
            
            # Determine the maximum length of amounts or charges lists
            max_length = max(len(amounts), len(charges))
            
            # Write data rows
            for i in range(max_length):
                amount_val = amounts[i] if i < len(amounts) else ''
                charge_val = charges[i] if i < len(charges) else ''
                
                # Calculate the sum of amount and charge in this row
                row_sum = 0
                
                # Process amount value if present
                if amount_val:
                    # Remove any currency symbol
                    amount_numeric = re.sub(r'[€$£¥]', '', amount_val)
                    # Replace comma with period if needed
                    if decimal_separator == ',':
                        amount_numeric = amount_numeric.replace(',', '.')
                    # Convert to float and add to row sum
                    try:
                        row_sum += float(amount_numeric)
                    except ValueError:
                        pass
                
                # Process charge value if present
                if charge_val:
                    # Remove any currency symbol
                    charge_numeric = re.sub(r'[€$£¥]', '', charge_val)
                    # Replace comma with period if needed
                    if decimal_separator == ',':
                        charge_numeric = charge_numeric.replace(',', '.')
                    # Convert to float and add to row sum
                    try:
                        row_sum += float(charge_numeric)
                    except ValueError:
                        pass
                
                # Format the row sum
                sum_val = str(row_sum) if (amount_val or charge_val) else ''
                
                writer.writerow([amount_val, charge_val, sum_val])

        # Send the file to the user
        with open(filename, 'rb') as file:
            update.message.reply_document(
                document=file,
                filename=filename,
                caption=f"📊 CSV export with {len(amounts)} amounts, {len(charges)} charges, and row-by-row sums."
            )

        # Clean up the file
        os.remove(filename)

    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        update.message.reply_text(
            "❗ Sorry, there was an error creating your CSV file. Please try again later."
        )

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
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    current_settings = (
        f"⚙️ <b>Current Settings</b>\n\n"
        f"🔢 Decimal Separator: '{preferences['decimal_separator']}'\n"
        f"💱 Include Currency: {'Yes' if preferences['include_currency'] else 'No'}\n"
        f"📋 Output Format: {preferences['output_format'].capitalize()}\n"
        f"🔕 Silent Collection: {'Yes' if preferences['silent_collection'] else 'No'}\n\n"
        f"Click below to change settings:"
    )

    update.message.reply_text(
        current_settings,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

def button_callback(update: Update, context) -> None:
    """Handle button presses from the settings menu."""
    query = update.callback_query
    user_id = query.from_user.id

    # Initialize user preferences if not already set
    if user_id not in user_preferences:
        user_preferences[user_id] = DEFAULT_PREFERENCES.copy()

    # Get the callback data
    data = query.data

    # Update preferences based on callback data
    if data.startswith('set_decimal_'):
        user_preferences[user_id]['decimal_separator'] = data[-1]
    elif data == 'toggle_currency':
        user_preferences[user_id]['include_currency'] = not user_preferences[user_id]['include_currency']
    elif data.startswith('set_format_'):
        user_preferences[user_id]['output_format'] = data[11:]
    elif data == 'toggle_silent':
        user_preferences[user_id]['silent_collection'] = not user_preferences[user_id]['silent_collection']

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

def main():
    """Start the bot."""
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

    # Add message handler
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, collect_message))

    # Add callback query handler
    dp.add_handler(CallbackQueryHandler(button_callback))

    # Keep the bot alive by starting the Flask server
    keep_alive()

    # Start the Bot
    updater.start_polling()
    logging.info("Bot started successfully")

    # Run the bot until you press Ctrl-C
    updater.idle()

if __name__ == '__main__':
    main()
