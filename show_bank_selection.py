def show_bank_selection(update: Update, context) -> None:
    """Show bank selection keyboard for CSV export."""
    # Get user ID
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message
    else:
        user_id = update.effective_user.id
        message = update.message
    
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
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        update.callback_query.edit_message_text(
            text="🏦 Please select a bank for deposit:",
            reply_markup=reply_markup
        )
    else:
        # Otherwise, send a new message
        message.reply_text(
            "🏦 Please select a bank for deposit:",
            reply_markup=reply_markup
        )