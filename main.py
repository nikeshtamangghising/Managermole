import re
import logging
import os
import time
import csv
import json
import socket
import threading
import random
import atexit
import sys
import psutil
import fcntl
import errno
import signal
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
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
LOCK_PORT = 10001
LOCK_FILE = "bot.lock"
PID_FILE = "bot.pid"

# Replace with your actual bot token - consider using an environment variable instead
BOT_TOKEN = os.environ.get('BOT_TOKEN')

# Global variables
bot_updater = None
bot_lock_socket = None
SHUTDOWN_IN_PROGRESS = False
BOT_INSTANCE_LOCK = threading.Lock()
lock_file_handle = None

# User data dictionaries
user_messages = {}
user_preferences = {}
DEFAULT_PREFERENCES = {
    'decimal_separator': '.',
    'include_currency': True,
    'output_format': 'simple',
    'silent_collection': False
}

def signal_handler(sig, frame):
    """Handle termination signals to ensure graceful shutdown."""
    logging.info(f"Received signal {sig}, initiating graceful shutdown...")
    global bot_updater, bot_lock_socket
    graceful_shutdown(bot_updater, bot_lock_socket)
    sys.exit(0)

def acquire_file_lock():
    """Acquire an exclusive file lock."""
    global lock_file_handle
    try:
        # First check if lock file exists and contains a valid PID
        if os.path.exists(LOCK_FILE):
            try:
                with open(LOCK_FILE, 'r') as f:
                    pid = int(f.read().strip())
                    # Check if process with this PID exists
                    try:
                        os.kill(pid, 0)  # Signal 0 just checks if process exists
                        logging.warning(f"Process with PID {pid} already holds the lock")
                        return False
                    except OSError:
                        # Process doesn't exist, we can proceed
                        logging.info(f"Stale lock file found with PID {pid}, proceeding")
                        # Don't remove the file here, we'll overwrite it
            except (ValueError, IOError) as e:
                logging.warning(f"Invalid lock file found: {e}, proceeding")
                # Don't remove the file here, we'll overwrite it
        
        # Try to acquire the lock
        lock_file_handle = open(LOCK_FILE, 'w')
        fcntl.flock(lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file_handle.write(str(os.getpid()))
        lock_file_handle.flush()
        logging.info(f"Successfully acquired file lock with PID {os.getpid()}")
        return True
    except IOError as e:
        if e.errno == errno.EAGAIN:
            logging.error("Another instance is already running")
            return False
        raise
    except Exception as e:
        logging.error(f"Error acquiring file lock: {e}")
        return False

def release_file_lock():
    """Release the file lock."""
    global lock_file_handle
    try:
        if lock_file_handle:
            fcntl.flock(lock_file_handle, fcntl.LOCK_UN)
            lock_file_handle.close()
            lock_file_handle = None
    except Exception as e:
        logging.error(f"Error releasing file lock: {e}")

def kill_existing_instances():
    """Kill any existing instances of the bot."""
    current_pid = os.getpid()
    
    # First try to kill using PID file
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                pid = int(f.read().strip())
                if pid != current_pid:
                    try:
                        os.kill(pid, 9)  # SIGKILL
                        logging.info(f"Killed process with PID {pid}")
                    except OSError:
                        pass
        except Exception as e:
            logging.error(f"Error reading PID file: {e}")
        finally:
            try:
                os.remove(PID_FILE)
            except:
                pass
    
    # Then try to kill using psutil
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] == 'python' and any('main.py' in cmd for cmd in proc.info['cmdline']):
                if proc.info['pid'] != current_pid:
                    logging.info(f"Killing existing bot instance with PID {proc.info['pid']}")
                    proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

def cleanup_resources():
    """Clean up all resources."""
    try:
        # Clean up webhook
        cleanup_webhook()
        
        # Clean up sockets
        cleanup_sockets()
        
        # Clean up lock files
        cleanup_lock_file()
        
        # Release file lock
        release_file_lock()
        
        # Release thread lock if we're holding it
        global BOT_INSTANCE_LOCK
        if BOT_INSTANCE_LOCK.locked():
            try:
                BOT_INSTANCE_LOCK.release()
                logging.info("Released thread lock during cleanup")
            except Exception as lock_error:
                logging.error(f"Error releasing thread lock: {lock_error}")
        
        # Remove PID file
        if os.path.exists(PID_FILE):
            try:
                os.remove(PID_FILE)
                logging.info("Removed PID file")
            except Exception as pid_error:
                logging.error(f"Error removing PID file: {pid_error}")
            
        logging.info("All resources cleaned up")
    except Exception as e:
        logging.error(f"Error during cleanup: {e}")
        import traceback
        logging.error(traceback.format_exc())

def cleanup_lock_file():
    """Clean up the lock file if it exists."""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logging.info("Removed lock file")
    except Exception as e:
        logging.error(f"Error cleaning up lock file: {e}")

def cleanup_webhook():
    """Clean up any existing webhooks for the bot with improved error handling."""
    max_attempts = 3
    attempt = 0
    while attempt < max_attempts:
        try:
            attempt += 1
            logging.info(f"Webhook cleanup attempt {attempt}/{max_attempts}")
            
            # Create a new bot instance for cleanup to avoid conflicts
            cleanup_bot = Bot(BOT_TOKEN)
            
            # First check if webhook exists
            webhook_info = cleanup_bot.get_webhook_info()
            if not webhook_info.url:
                logging.info("No webhook found, skipping cleanup")
                return
                
            # Use drop_pending_updates=True to ensure clean state
            cleanup_bot.delete_webhook(drop_pending_updates=True)
            logging.info("Cleaned up existing webhook")
            
            # Verify webhook was deleted
            time.sleep(3)  # Wait before verification
            webhook_info = cleanup_bot.get_webhook_info()
            if not webhook_info.url:
                logging.info("Webhook deletion verified")
                break
            else:
                logging.warning("Webhook still exists after deletion attempt")
            
            # Add a longer delay to ensure webhook deletion is processed
            time.sleep(5)
            
            # Clean up the bot instance
            del cleanup_bot
            
        except Exception as e:
            logging.error(f"Error cleaning up webhook (attempt {attempt}/{max_attempts}): {e}")
            import traceback
            logging.error(traceback.format_exc())
            time.sleep(3)  # Wait before retrying
    
    if attempt >= max_attempts:
        logging.warning("Maximum webhook cleanup attempts reached")
        # Try one last time with a different approach
        try:
            final_bot = Bot(BOT_TOKEN)
            final_bot.delete_webhook(drop_pending_updates=True)
            logging.info("Final webhook cleanup attempt completed")
            del final_bot
        except Exception as final_error:
            logging.error(f"Final webhook cleanup attempt failed: {final_error}")
            pass


def cleanup_sockets():
    """Clean up any existing socket connections."""
    try:
        # Close the lock socket if it exists
        global bot_lock_socket
        if bot_lock_socket:
            try:
                # Check if socket is still valid before closing
                if hasattr(bot_lock_socket, 'fileno') and bot_lock_socket.fileno() >= 0:
                    bot_lock_socket.close()
                    logging.info("Closed lock socket")
                else:
                    logging.info("Lock socket already closed or invalid")
                # Set to None to prevent further usage
                bot_lock_socket = None
            except OSError as e:
                if e.errno == errno.EBADF:  # Bad file descriptor
                    logging.info("Socket already closed or invalid")
                    bot_lock_socket = None
                else:
                    logging.error(f"Error closing lock socket: {e}")
            except Exception as e:
                logging.error(f"Error closing lock socket: {e}")
        
        # Close any other sockets we might have created
        if hasattr(socket, '_socketobject'):
            for sock in socket._socketobject._instances:
                try:
                    if hasattr(sock, 'fileno') and sock.fileno() >= 0:
                        sock.close()
                except:
                    pass
    except Exception as e:
        logging.error(f"Error during socket cleanup: {e}")

def create_socket_lock():
    """Create a socket-based lock to ensure only one instance of the bot runs."""
    # First, try to kill any existing instances
    kill_existing_instances()
    logging.info("Waiting for existing processes to terminate")
    time.sleep(10)  # Longer wait for processes to be killed
    
    # Create a new socket for locking
    lock_socket = None
    max_attempts = 5  # Increased max attempts
    attempt = 0
    
    while attempt < max_attempts:
        attempt += 1
        logging.info(f"Socket lock attempt {attempt}/{max_attempts}")
        
        try:
            # Close any existing socket first
            if lock_socket:
                try:
                    if hasattr(lock_socket, 'fileno') and lock_socket.fileno() >= 0:
                        lock_socket.close()
                        logging.info("Closed previous socket attempt")
                except Exception as close_error:
                    logging.error(f"Error closing previous socket: {close_error}")
            
            # Create a new socket for locking
            lock_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            # Set socket options for better exclusivity
            lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                lock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            
            # Keep socket in blocking mode for reliability
            lock_socket.setblocking(True)
            
            # Try to bind to the lock port
            try:
                # Try to bind with a timeout to avoid hanging
                bind_thread = threading.Thread(target=lambda: lock_socket.bind(('localhost', LOCK_PORT)))
                bind_thread.daemon = True
                bind_thread.start()
                bind_thread.join(5)  # Wait up to 5 seconds for binding
                
                if bind_thread.is_alive():
                    logging.error("Binding operation timed out")
                    # Force close the socket
                    try:
                        lock_socket.close()
                    except:
                        pass
                    lock_socket = None
                    raise socket.error("Binding operation timed out")
                
                logging.info(f"Successfully bound to port {LOCK_PORT}")
            except socket.error as bind_error:
                logging.error(f"Failed to bind to lock port: {bind_error}")
                # Port is already in use, another instance is running
                if lock_socket:
                    try:
                        lock_socket.close()
                    except:
                        pass
                    lock_socket = None
                
                # If this is our last attempt, give up
                if attempt >= max_attempts:
                    logging.error("Maximum socket lock attempts reached, giving up")
                    return None
                    
                # Otherwise wait and try again with exponential backoff
                wait_time = min(5 * (2 ** (attempt - 1)) + random.uniform(1, 5), 60)  # Cap at 60 seconds
                logging.info(f"Waiting {wait_time:.1f} seconds before next attempt")
                time.sleep(wait_time)
                continue
            
            # Set a reasonable timeout that won't cause issues with heartbeats
            lock_socket.settimeout(30)  # Shorter timeout for better responsiveness
            
            # Verify socket is valid before proceeding
            if not hasattr(lock_socket, 'fileno') or lock_socket.fileno() < 0:
                logging.error("Created socket has invalid file descriptor")
                return None
            
            # Create lock file with PID
            try:
                with open(LOCK_FILE, 'w') as f:
                    f.write(str(os.getpid()))
                logging.info(f"Created lock file with PID {os.getpid()}")
            except Exception as file_error:
                logging.error(f"Error creating lock file: {file_error}")
                # Continue anyway, socket lock is still valid
            
            # Register cleanup function
            atexit.register(cleanup_lock_file)
            
            # Start a more robust heartbeat mechanism
            def keep_socket_alive(socket_ref):
                # Use a local reference to the socket to prevent issues with global variable changes
                local_socket = socket_ref
                running = True
                heartbeat_count = 0
                
                while running:
                    try:
                        # Check if socket is still valid before sending heartbeat
                        if local_socket is None or not hasattr(local_socket, 'fileno') or local_socket.fileno() < 0:
                            logging.warning("Socket appears to be closed or invalid, stopping heartbeat")
                            break
                        
                        # Send heartbeat
                        heartbeat_count += 1
                        local_socket.sendto(f"heartbeat-{heartbeat_count}".encode(), ('localhost', LOCK_PORT))
                        
                        # Log occasional heartbeat status
                        if heartbeat_count % 20 == 0:  # Log every ~60 seconds
                            logging.info(f"Socket heartbeat still active (count: {heartbeat_count})")
                            
                        time.sleep(3)  # More frequent heartbeats
                    except OSError as e:
                        # Handle specific socket errors
                        if e.errno == errno.EBADF:  # Bad file descriptor
                            logging.warning("Socket descriptor is no longer valid, stopping heartbeat")
                        else:
                            logging.error(f"Socket error in heartbeat: {e}")
                        break
                    except Exception as e:
                        logging.error(f"Heartbeat error: {e}")
                        break
            
            heartbeat_thread = threading.Thread(target=keep_socket_alive, args=(lock_socket,), daemon=True)
            heartbeat_thread.start()
            
            logging.info(f"Successfully acquired socket lock on port {LOCK_PORT}")
            return lock_socket
                
        except Exception as e:
            logging.error(f"Failed to acquire socket lock: {e}")
            try:
                if lock_socket:
                    lock_socket.close()
            except:
                pass
                
            # If this is our last attempt, give up
            if attempt >= max_attempts:
                logging.error("Maximum socket lock attempts reached, giving up")
                return None
                
            # Otherwise wait and try again
            wait_time = 3 * attempt
            logging.info(f"Waiting {wait_time} seconds before next attempt")
            time.sleep(wait_time)

def graceful_shutdown(updater=None, lock_socket=None):
    """Perform a graceful shutdown of the bot with enhanced cleanup."""
    global SHUTDOWN_IN_PROGRESS, bot_lock_socket, BOT_INSTANCE_LOCK, bot_updater
    
    # Use a timeout to prevent hanging during shutdown
    shutdown_timeout = threading.Timer(30, lambda: os._exit(1))
    shutdown_timeout.daemon = True
    
    try:
        if SHUTDOWN_IN_PROGRESS:
            logging.info("Shutdown already in progress, skipping duplicate shutdown")
            return
            
        SHUTDOWN_IN_PROGRESS = True
        logging.info("Starting graceful shutdown...")
        
        # Start the timeout timer
        shutdown_timeout.start()
        
        # First stop the updater to prevent further conflicts
        if updater:
            try:
                # Reset update fetcher state first
                if hasattr(updater, 'dispatcher') and hasattr(updater.dispatcher, '_update_fetcher'):
                    if hasattr(updater.dispatcher._update_fetcher, '_last_update_id'):
                        updater.dispatcher._update_fetcher._last_update_id = 0
                        logging.info("Reset update fetcher ID")
                    if hasattr(updater.dispatcher._update_fetcher, 'running'):
                        updater.dispatcher._update_fetcher.running = False
                        logging.info("Stopped update fetcher")
                
                # Stop the updater with a timeout to prevent hanging
                stop_thread = threading.Thread(target=updater.stop)
                stop_thread.daemon = True
                stop_thread.start()
                stop_thread.join(10)  # Wait up to 10 seconds for clean stop
                
                if stop_thread.is_alive():
                    logging.warning("Updater stop operation timed out")
                else:
                    logging.info("Stopped updater successfully")
            except Exception as e:
                logging.error(f"Error stopping updater: {e}")
        
        # Delete webhook after stopping updater
        try:
            # Create a new bot instance for webhook deletion to avoid conflicts
            cleanup_bot = Bot(BOT_TOKEN)
            webhook_info = cleanup_bot.get_webhook_info()
            
            if webhook_info.url:
                # Use a thread with timeout for webhook deletion
                def delete_webhook():
                    try:
                        cleanup_bot.delete_webhook(drop_pending_updates=True)
                        logging.info("Deleted webhook during shutdown")
                    except Exception as webhook_error:
                        logging.error(f"Error in webhook deletion thread: {webhook_error}")
                
                webhook_thread = threading.Thread(target=delete_webhook)
                webhook_thread.daemon = True
                webhook_thread.start()
                webhook_thread.join(10)  # Wait up to 10 seconds for webhook deletion
                
                if webhook_thread.is_alive():
                    logging.warning("Webhook deletion timed out")
                
                # Add a delay to ensure webhook deletion is processed
                time.sleep(2)
            else:
                logging.info("No webhook found during shutdown")
                
            # Clean up the bot instance
            del cleanup_bot
        except Exception as e:
            logging.error(f"Error handling webhook during shutdown: {e}")
        
        # Release thread lock
        if BOT_INSTANCE_LOCK.locked():
            try:
                BOT_INSTANCE_LOCK.release()
                logging.info("Released thread lock")
            except Exception as e:
                logging.error(f"Error releasing thread lock: {e}")
        
        # Close socket lock
        if lock_socket:
            try:
                # Check if socket is still valid before closing
                if hasattr(lock_socket, 'fileno') and lock_socket.fileno() >= 0:
                    lock_socket.close()
                    logging.info("Closed socket lock")
                else:
                    logging.info("Socket lock already closed or invalid")
                # Set global variable to None to prevent further usage
                bot_lock_socket = None
            except OSError as e:
                if e.errno == errno.EBADF:  # Bad file descriptor
                    logging.info("Socket lock already closed or invalid")
                else:
                    logging.error(f"Error closing socket lock: {e}")
            except Exception as e:
                logging.error(f"Error closing socket lock: {e}")
        
        # Clean up lock file
        cleanup_lock_file()
        
        # Release file lock
        release_file_lock()
        
        # Remove PID file
        if os.path.exists(PID_FILE):
            try:
                os.remove(PID_FILE)
                logging.info("Removed PID file during shutdown")
            except Exception as pid_error:
                logging.error(f"Error removing PID file: {pid_error}")
        
        # Additional cleanup
        try:
            # Force cleanup of any remaining sockets
            for sock in [s for s in socket._socketobject._instances if hasattr(s, 'fileno') and s.fileno() > 0]:
                try:
                    sock.close()
                    logging.debug("Closed additional socket")
                except:
                    pass
        except Exception as e:
            logging.error(f"Error during additional socket cleanup: {e}")
            
        # Kill any remaining bot processes as a last resort
        try:
            kill_existing_instances()
        except Exception as kill_error:
            logging.error(f"Error killing existing instances during shutdown: {kill_error}")
            
    except Exception as e:
        logging.error(f"Error during shutdown: {e}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        # Cancel the timeout timer if it's still running
        if shutdown_timeout.is_alive():
            shutdown_timeout.cancel()
        
        SHUTDOWN_IN_PROGRESS = False
        logging.info("Shutdown complete")
        
        # Set global variables to None to prevent further usage
        bot_updater = None

def error_handler(update, context):
    """Handle errors in the dispatcher with improved conflict resolution."""
    try:
        if isinstance(context.error, Conflict):
            logging.warning("Conflict error: Another instance of the bot is already running")
            logging.info(f"Detailed conflict error: {context.error}")
            logging.info("Attempting to resolve conflict situation...")
            
            # Check if we should terminate this instance
            global BOT_INSTANCE_LOCK, bot_updater, bot_lock_socket, SHUTDOWN_IN_PROGRESS
            
            # Prevent multiple shutdown attempts
            if SHUTDOWN_IN_PROGRESS:
                logging.info("Shutdown already in progress, skipping additional conflict resolution")
                return
                
            SHUTDOWN_IN_PROGRESS = True
            
            # Check if we hold the thread lock - if not, we should terminate
            if not BOT_INSTANCE_LOCK.locked():
                logging.warning("Thread lock not held by this instance - this instance should terminate")
            else:
                logging.warning("Thread lock held by this instance but conflict detected - possible race condition")
                # Release the lock since we're going to terminate anyway
                try:
                    BOT_INSTANCE_LOCK.release()
                    logging.info("Released thread lock during conflict resolution")
                except Exception as lock_error:
                    logging.error(f"Error releasing thread lock: {lock_error}")
                
            # Always terminate this instance if we encounter a conflict
            logging.warning("Conflict detected - terminating this instance")
            
            # More aggressive cleanup
            try:
                # First stop the global updater if it exists
                if bot_updater:
                    try:
                        bot_updater.stop()
                        logging.info("Stopped global updater during conflict resolution")
                    except Exception as updater_error:
                        logging.error(f"Error stopping global updater: {updater_error}")
                
                # Also try to stop the context's updater if different
                if hasattr(context.dispatcher, 'updater') and context.dispatcher.updater:
                    try:
                        if bot_updater is None or context.dispatcher.updater != bot_updater:  # Only if different from global
                            context.dispatcher.updater.stop()
                            logging.info("Stopped context updater during conflict resolution")
                    except Exception as updater_error:
                        logging.error(f"Error stopping context updater: {updater_error}")
                
                # Delete webhook with drop_pending_updates to ensure clean state
                if hasattr(context, 'bot'):
                    try:
                        webhook_info = context.bot.get_webhook_info()
                        if webhook_info.url:  # Only delete if webhook exists
                            context.bot.delete_webhook(drop_pending_updates=True)
                            logging.info("Deleted webhook during conflict resolution")
                            # Wait for webhook deletion to process
                            time.sleep(3)
                    except Exception as webhook_error:
                        logging.error(f"Error deleting webhook: {webhook_error}")
                
                # Reset update fetcher state
                if hasattr(context.dispatcher, '_update_fetcher'):
                    if hasattr(context.dispatcher._update_fetcher, '_last_update_id'):
                        context.dispatcher._update_fetcher._last_update_id = 0
                        logging.info("Reset update ID to 0")
                    
                    # Stop the update fetcher if it's running
                    if hasattr(context.dispatcher._update_fetcher, 'running'):
                        context.dispatcher._update_fetcher.running = False
                        logging.info("Stopped update fetcher")
                
                # Release thread lock if we're still holding it somehow
                if BOT_INSTANCE_LOCK.locked():
                    try:
                        BOT_INSTANCE_LOCK.release()
                        logging.info("Released thread lock during conflict resolution")
                    except Exception as lock_error:
                        logging.error(f"Error releasing thread lock: {lock_error}")
                
                # Close socket lock if it exists
                if bot_lock_socket:
                    try:
                        if hasattr(bot_lock_socket, 'fileno') and bot_lock_socket.fileno() >= 0:
                            bot_lock_socket.close()
                            logging.info("Closed socket lock during conflict resolution")
                        bot_lock_socket = None
                    except Exception as socket_error:
                        logging.error(f"Error closing socket lock: {socket_error}")
                
                # Perform full cleanup
                cleanup_resources()
                
                # Perform graceful shutdown
                graceful_shutdown(bot_updater, bot_lock_socket)
                
                # Wait to ensure cleanup is complete
                time.sleep(5)
                
                # Force exit this instance
                logging.info("Exiting due to conflict resolution")
                SHUTDOWN_IN_PROGRESS = False
                os._exit(1)  # Use os._exit for more forceful termination
                
            except Exception as e:
                logging.error(f"Failed to recover from conflict: {e}")
                import traceback
                logging.error(traceback.format_exc())
                # Still exit even if cleanup failed
                SHUTDOWN_IN_PROGRESS = False
                os._exit(1)  # Use os._exit for more forceful termination
                
        elif isinstance(context.error, NetworkError):
            logging.error(f"Network error: {context.error}. Waiting before retry.")
            backoff_time = 25 + (5 * random.random())
            logging.info(f"Backing off for {backoff_time:.1f} seconds")
            time.sleep(backoff_time)
        else:
            update_str = str(update) if update else "None"
            logging.error(f"Update {update_str} caused error: {context.error}")
            import traceback
            logging.error(f"Error traceback: {traceback.format_exc()}")
    except Exception as e:
        logging.error(f"Error in error handler: {e}")
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
    """Export the results as a CSV file with the format: Date, Deposit Amount, Bank Name, Paid To Host, Remaining Balance."""
    # Determine if this is called from a callback query or directly
    if hasattr(update, 'callback_query') and update.callback_query is not None:
        query = update.callback_query
        user_id = query.from_user.id
        message = query.message
    else:
        if hasattr(update, 'effective_user') and update.effective_user is not None:
            user_id = update.effective_user.id
            message = update.message
        elif hasattr(update, 'message') and update.message is not None:
            user_id = update.message.from_user.id
            message = update.message
        else:
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
        manual_remaining_balance = user_states[user_id].get('remaining_balance')
        csv_path = user_states[user_id].get('csv_path')
        
        if 'bank_deposits' in user_states[user_id] and user_states[user_id]['bank_deposits']:
            bank_deposits = user_states[user_id]['bank_deposits']
            
            for deposit in bank_deposits:
                deposit_amount = deposit['amount']
                deposit_str = str(int(deposit_amount) if deposit_amount.is_integer() else deposit_amount)
                amounts.append(deposit_str)
    else:
        csv_path = None
        manual_remaining_balance = None
        
    # Variable to store previous day's balance
    previous_balance = 0.0
    
    if manual_remaining_balance is not None:
        previous_balance = manual_remaining_balance
        logger.info(f"Using manually entered remaining balance: {previous_balance}")
    
    # Process all collected messages if not using manual input exclusively
    if not use_manual_input or not amounts:
        for message_data in user_messages[user_id]:
            message_text = message_data['text']
            matches = re.findall(pattern, message_text)

            for match in matches:
                currency, original_number, processed_number, value, has_decimal = extract_number_value(match, decimal_separator, message_text)
                extracted_value = f"{currency}{processed_number}" if currency and preferences['include_currency'] else processed_number

                if value > AMOUNT_THRESHOLD:
                    amounts.append(extracted_value)
                else:
                    charges.append(extracted_value)

    if not amounts and not charges:
        message.reply_text("❗ I couldn't find any numbers in your collected messages.")
        return

    # Calculate the total deposit (sum of amounts)
    total_deposit = 0
    
    if previous_balance > 0 and not (use_manual_input and 'bank_deposits' in user_states[user_id] and 
                                   any(d['bank'] == 'Previous Balance' for d in bank_deposits)):
        total_deposit += previous_balance
        
    for amount_str in amounts:
        numeric_str = re.sub(r'[€$£¥]', '', amount_str)
        if decimal_separator == ',':
            numeric_str = numeric_str.replace(',', '.')
        try:
            total_deposit += float(numeric_str)
        except ValueError:
            pass
    
    # Calculate the total paid (sum of charges)
    total_paid = 0
    for charge_str in charges:
        numeric_str = re.sub(r'[€$£¥]', '', charge_str)
        if decimal_separator == ',':
            numeric_str = numeric_str.replace(',', '.')
        try:
            total_paid += float(numeric_str)
        except ValueError:
            pass
    
    # Calculate the balance (total deposit - total paid)
    balance = total_deposit - total_paid

    # Determine the CSV file path
    if csv_path and os.path.isfile(csv_path):
        filename = csv_path
        file_exists = True
        user_csv_files[user_id] = csv_path
    else:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        current_month = datetime.now().strftime('%B_%Y')
        filename = os.path.join(current_dir, f"decimal_stripper_export_{current_month}.csv")
        file_exists = False
        user_csv_files[user_id] = filename

    try:
        if file_exists:
            mode = 'a'
        else:
            mode = 'w'
        
        with open(filename, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            if not file_exists:
                fieldnames = ['Date', 'Deposit Amount', 'Bank Name', 'Paid To Host', 'Remaining Balance']
                writer.writerow(fieldnames)
            
            # Write bank deposits first
            if bank_deposits:
                sorted_deposits = sorted(bank_deposits, key=lambda x: 0 if x['bank'] == 'Previous Balance' else 1)
                
                for i, deposit in enumerate(sorted_deposits):
                    bank_name = deposit['bank']
                    deposit_amount = deposit['amount']
                    deposit_amount_formatted = f"{float(deposit_amount):.2f}"
                    
                    if i == 0:
                        deposit_row = [current_date, deposit_amount_formatted, bank_name, '', '']
                    else:
                        deposit_row = ['', deposit_amount_formatted, bank_name, '', '']
                    writer.writerow(deposit_row)
            
            # Write charges with running sums
            running_paid = 0.0
            for i in range(len(charges)):
                charge_str = charges[i]
                numeric_str = re.sub(r'[€$£¥]', '', charge_str)
                if decimal_separator == ',':
                    numeric_str = numeric_str.replace(',', '.')
                try:
                    charge_value = float(numeric_str)
                    running_paid += charge_value
                    
                    # Write the charge row
                    charge_row = ['', '', '', f"{charge_value:.2f}", '']
                    writer.writerow(charge_row)
            
                    # Write the running sum row
                    sum_row = ['', '', '', f"Running Sum: {running_paid:.2f}", '']
                    writer.writerow(sum_row)
                except ValueError:
                    continue
            
            # Write final totals
            writer.writerow(['', '', '', '', ''])
            totals_row = ['', 'Total Deposit', '', 'Total Paid', 'Remaining Balance']
            writer.writerow(totals_row)
            values_row = ['', f"{total_deposit:.2f}", '', f"{total_paid:.2f}", f"{balance:.2f}"]
            writer.writerow(values_row)

        # Send the file to the user
        with open(filename, 'rb') as file:
            message.reply_document(
                document=file,
                filename=os.path.basename(filename),
                caption=f"📊 CSV export with the format: Date, Deposit Amount, Bank Name, Paid To Host, Remaining Balance.\n\nThe file includes:\n- Bank deposits with their respective amounts\n- Individual payments with running sums\n- Final totals and remaining balance"
            )

        if not csv_path:
            os.remove(filename)

        if user_id in user_states:
            del user_states[user_id]

    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        message.reply_text(f"❗ Sorry, there was an error creating your CSV file: {str(e)}")
        
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

def main():
    """Start the bot with enhanced instance management."""
    global bot_updater, bot_lock_socket, BOT_INSTANCE_LOCK, SHUTDOWN_IN_PROGRESS
    
    # Reset shutdown flag at the beginning
    SHUTDOWN_IN_PROGRESS = False
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Kill any existing instances first - more aggressive approach
    logging.info("Killing any existing bot instances")
    kill_existing_instances()
    
    # Clean up any existing resources
    logging.info("Cleaning up any existing resources")
    cleanup_resources()
    
    # Wait for resources to be available
    logging.info("Waiting for resources to be available")
    time.sleep(10)
    
    # Acquire thread lock first to prevent race conditions
    logging.info("Attempting to acquire thread lock")
    if not BOT_INSTANCE_LOCK.acquire(timeout=30):  # Reasonable timeout
        logging.error("Failed to acquire thread lock - exiting")
        return
        
    logging.info("Thread lock acquired successfully")
    
    # Try to acquire file lock
    logging.info("Attempting to acquire file lock")
    if not acquire_file_lock():
        logging.error("Failed to acquire file lock - exiting")
        if BOT_INSTANCE_LOCK.locked():
            BOT_INSTANCE_LOCK.release()
        return
    
    logging.info("File lock acquired successfully")
    
    # Create socket lock as an additional layer of protection
    logging.info("Attempting to create socket lock")
    bot_lock_socket = create_socket_lock()
    if not bot_lock_socket:
        logging.error("Failed to create socket lock - exiting")
        release_file_lock()
        if BOT_INSTANCE_LOCK.locked():
            BOT_INSTANCE_LOCK.release()
        return
    
    logging.info("Socket lock acquired successfully - all locks in place")
    logging.info("Bot instance is now the primary instance")
    
    # Write PID to file
    try:
        with open(PID_FILE, 'w') as f:
            f.write(str(os.getpid()))
    except Exception as e:
        logging.error(f"Error writing PID file: {e}")
        cleanup_resources()  # Ensure cleanup happens
        return
    
    updater = None
    
    try:
        if not BOT_TOKEN:
            logging.error("Bot token not found")
            return
        
        # Create updater with correct timeout parameters
        updater = Updater(BOT_TOKEN, request_kwargs={'read_timeout': 30, 'connect_timeout': 30})
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

        # Start keep-alive
        keep_alive()
        
        # Start bot with improved conflict handling
        max_retries = 10  # Fewer retries but more thorough cleanup between attempts
        retry_count = 0
        backoff_time = 30  # Longer initial backoff
        
        while retry_count < max_retries:
            try:
                retry_count += 1
                logging.info(f"Attempt {retry_count}/{max_retries} to start bot")
                
                # Thorough cleanup before each attempt
                cleanup_resources()
                kill_existing_instances()  # Kill any competing instances
                time.sleep(20)  # Longer wait between attempts
                
                # Reset update fetcher state to avoid conflicts
                if hasattr(dp, '_update_fetcher'):
                    if hasattr(dp._update_fetcher, '_last_update_id'):
                        dp._update_fetcher._last_update_id = 0
                    if hasattr(dp._update_fetcher, 'running'):
                        dp._update_fetcher.running = False
                    logging.info("Reset update fetcher state")
                
                # Make sure webhook is deleted before polling
                try:
                    # Create a new bot instance for webhook deletion to avoid conflicts
                    cleanup_bot = Bot(BOT_TOKEN)
                    webhook_info = cleanup_bot.get_webhook_info()
                    
                    if webhook_info.url:
                        # Use a thread with timeout for webhook deletion
                        def delete_webhook():
                            try:
                                cleanup_bot.delete_webhook(drop_pending_updates=True)
                                logging.info("Deleted webhook before polling")
                            except Exception as webhook_error:
                                logging.error(f"Error in webhook deletion thread: {webhook_error}")
                        
                        webhook_thread = threading.Thread(target=delete_webhook)
                        webhook_thread.daemon = True
                        webhook_thread.start()
                        webhook_thread.join(10)  # Wait up to 10 seconds for webhook deletion
                        
                        if webhook_thread.is_alive():
                            logging.warning("Webhook deletion timed out")
                        
                        # Add a delay to ensure webhook deletion is processed
                        time.sleep(10)
                    else:
                        logging.info("No webhook found, proceeding with polling")
                        
                    # Clean up the bot instance
                    del cleanup_bot
                except Exception as e:
                    logging.error(f"Error checking/deleting webhook: {e}")
                
                # Start polling with correct parameters and in a separate thread with timeout
                def start_polling():
                    try:
                        updater.start_polling(
                            timeout=60,  # Reasonable timeout
                            drop_pending_updates=True,
                            allowed_updates=['message', 'callback_query', 'chat_member'],
                            bootstrap_retries=3  # Fewer bootstrap retries but more thorough cleanup between attempts
                        )
                        logging.info("Polling started successfully")
                    except Exception as polling_error:
                        logging.error(f"Error in polling thread: {polling_error}")
                
                polling_thread = threading.Thread(target=start_polling)
                polling_thread.daemon = True
                polling_thread.start()
                polling_thread.join(30)  # Wait up to 30 seconds for polling to start
                
                if polling_thread.is_alive():
                    # Thread is still running, which is good - it means polling is active
                    logging.info("Bot started successfully")
                    break
                else:
                    # Thread completed, which might indicate an error
                    logging.warning("Polling thread completed unexpectedly, will retry")
                    time.sleep(5)  # Wait before checking for conflicts
                    continue
                
            except Conflict as ce:
                logging.error(f"Conflict error on attempt {retry_count}/{max_retries}: {ce}")
                
                # Aggressive cleanup
                try:
                    # First stop the updater if it's running
                    if updater and hasattr(updater, 'stop'):
                        try:
                            # Stop the updater with a timeout to prevent hanging
                            stop_thread = threading.Thread(target=updater.stop)
                            stop_thread.daemon = True
                            stop_thread.start()
                            stop_thread.join(10)  # Wait up to 10 seconds for clean stop
                            
                            if stop_thread.is_alive():
                                logging.warning("Updater stop operation timed out")
                            else:
                                logging.info("Stopped updater during conflict resolution")
                        except Exception as updater_error:
                            logging.error(f"Error stopping updater: {updater_error}")
                    
                    # Reset update fetcher state
                    if hasattr(dp, '_update_fetcher'):
                        if hasattr(dp._update_fetcher, 'running'):
                            dp._update_fetcher.running = False
                        if hasattr(dp._update_fetcher, '_last_update_id'):
                            dp._update_fetcher._last_update_id = 0
                            logging.info("Reset update fetcher during conflict resolution")
                    
                    # Delete webhook to ensure clean state
                    try:
                        # Create a new bot instance for webhook deletion
                        cleanup_bot = Bot(BOT_TOKEN)
                        webhook_info = cleanup_bot.get_webhook_info()
                        
                        if webhook_info.url:
                            cleanup_bot.delete_webhook(drop_pending_updates=True)
                            logging.info("Deleted webhook during conflict resolution")
                            time.sleep(5)  # Wait for webhook deletion to process
                        
                        # Clean up the bot instance
                        del cleanup_bot
                    except Exception as webhook_error:
                        logging.error(f"Error deleting webhook: {webhook_error}")
                    
                    # Release thread lock if we're holding it
                    if BOT_INSTANCE_LOCK.locked():
                        try:
                            BOT_INSTANCE_LOCK.release()
                            logging.info("Released thread lock during conflict resolution")
                        except Exception as lock_error:
                            logging.error(f"Error releasing thread lock: {lock_error}")
                    
                    # Close socket lock if it exists
                    if bot_lock_socket:
                        try:
                            if hasattr(bot_lock_socket, 'fileno') and bot_lock_socket.fileno() >= 0:
                                bot_lock_socket.close()
                                logging.info("Closed socket lock during conflict resolution")
                            bot_lock_socket = None
                        except Exception as socket_error:
                            logging.error(f"Error closing socket lock: {socket_error}")
                    
                    # Perform full cleanup
                    cleanup_resources()
                    
                    # Kill any existing instances
                    kill_existing_instances()
                    
                    # Wait to ensure cleanup is complete
                    time.sleep(10)  # Longer wait to ensure cleanup is complete
                            
                except Exception as cleanup_error:
                    logging.error(f"Error during conflict cleanup: {cleanup_error}")
                    import traceback
                    logging.error(traceback.format_exc())
                
                if retry_count >= max_retries:
                    logging.error("Maximum retry attempts reached")
                    # Make sure we release locks before exiting
                    cleanup_resources()  # Final cleanup attempt
                    return
                    
                # Increment retry count after cleanup
                retry_count += 1
                    
                # Exponential backoff with randomization
                backoff_time = min(backoff_time * 1.5 + random.uniform(10, 20), 180)  # Cap at 180 seconds
                logging.info(f"Waiting {backoff_time:.1f} seconds before next attempt")
                time.sleep(backoff_time)
                
                # Kill any existing instances before retrying
                kill_existing_instances()
                cleanup_resources()
                time.sleep(10)  # Longer wait for cleanup to complete
                
            except Exception as e:
                logging.error(f"Failed to start bot: {e}")
                import traceback
                logging.error(traceback.format_exc())
                
                # Perform cleanup in case of general errors too
                try:
                    # Stop the updater if it's running
                    if updater and hasattr(updater, 'stop'):
                        try:
                            updater.stop()
                            logging.info("Stopped updater after general error")
                        except Exception as stop_error:
                            logging.error(f"Error stopping updater: {stop_error}")
                    
                    # Clean up resources
                    cleanup_resources()
                except Exception as cleanup_error:
                    logging.error(f"Error during cleanup after general error: {cleanup_error}")
                
                retry_count += 1
                # Use exponential backoff with randomization for general errors too
                backoff_time = min(backoff_time * 2 + random.uniform(5, 15), 180)  # Cap at 180 seconds
                logging.info(f"Waiting {backoff_time:.1f} seconds before next attempt")
                time.sleep(backoff_time)
                
                # Kill any existing instances before retrying
                kill_existing_instances()
                time.sleep(5)  # Wait for cleanup to complete
                
                if retry_count >= max_retries:
                    logging.error("Maximum retry attempts reached")
                    cleanup_resources()  # Final cleanup attempt
                    return

        # Run the bot
        logging.info("Bot is now running")
        updater.idle()
        
    except KeyboardInterrupt:
        logging.info("Bot stopping due to keyboard interrupt")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        logging.info("Performing cleanup")
        cleanup_resources()
        graceful_shutdown(updater, bot_lock_socket)
        logging.info("Cleanup complete")

if __name__ == "__main__":
    try:
        # Register signal handlers for graceful shutdown
        import signal
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Kill any existing instances before starting
        kill_existing_instances()
        
        # Clean up any existing resources
        cleanup_resources()
        
        # Wait for resources to be available
        time.sleep(10)
        
        # Start the bot
        main()
    except Exception as e:
        logging.critical(f"Critical error during startup: {e}")
        import traceback
        logging.critical(traceback.format_exc())
        sys.exit(1)