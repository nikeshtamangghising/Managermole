from flask import Flask, render_template_string
from threading import Thread
import logging
import os
from datetime import datetime
import psutil
import requests
import time
import socket

app = Flask('')

# HTML template for the home page
HOME_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Bot Status Dashboard</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f0f2f5;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .status {
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 10px;
        }
        .online {
            background-color: #e6ffe6;
            color: #006600;
        }
        .stat-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        .stat-card {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
        h1 { color: #1a73e8; }
        h2 { color: #5f6368; font-size: 1.2em; }
        .refresh {
            background-color: #1a73e8;
            color: white;
            padding: 10px 20px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            text-decoration: none;
            display: inline-block;
            margin-top: 20px;
        }
        .refresh:hover {
            background-color: #1557b0;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Bot Status Dashboard</h1>
        
        <div class="status online">
            <h2>✅ Bot Status: Online</h2>
            <p>Last checked: {{ current_time }}</p>
        </div>

        <div class="stat-grid">
            <div class="stat-card">
                <h2>💻 System Stats</h2>
                <p>CPU Usage: {{ cpu_usage }}%</p>
                <p>Memory Usage: {{ memory_usage }}%</p>
                <p>Uptime: {{ uptime }}</p>
            </div>

            <div class="stat-card">
                <h2>🌐 Server Info</h2>
                <p>Port: {{ port }}</p>
                <p>Host: {{ host }}</p>
                <p>Debug Mode: {{ debug }}</p>
                <p>Permanent Replit URL: <span id="replit-url">{{ replit_url }}</span></p>
                <p style="color: #1557b0; font-size: 0.9em;">⚡ This is your permanent URL for UptimeRobot</p>
                <button onclick="copyUrl()" class="refresh" style="background-color: #34a853;">📋 Copy URL</button>
                <script>
                    function copyUrl() {
                        const url = document.getElementById('replit-url').textContent;
                        navigator.clipboard.writeText(url);
                        alert('URL copied to clipboard!');
                    }
                </script>
            </div>
        </div>

        <a href="/" class="refresh">🔄 Refresh Status</a>
    </div>
</body>
</html>
'''

@app.route('/')
def home():
    stats = {
        'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'cpu_usage': round(psutil.cpu_percent(), 1),
        'memory_usage': round(psutil.virtual_memory().percent, 1),
        'uptime': str(datetime.now() - START_TIME).split('.')[0],
        'port': os.getenv('PORT', 8080),
        'host': '0.0.0.0',
        'debug': 'Disabled',
        'replit_url': f'https://{os.getenv("REPL_SLUG", "unknown")}.{os.getenv("REPL_OWNER", "unknown")}.replit.co'
    }
    return render_template_string(HOME_TEMPLATE, **stats)

@app.route('/health')
def health():
    replit_url = f'https://{os.getenv("REPL_SLUG", "unknown")}.{os.getenv("REPL_OWNER", "unknown")}.replit.co'
    return {
        "status": "healthy",
        "message": "Bot is running",
        "timestamp": datetime.now().isoformat(),
        "uptime": str(datetime.now() - START_TIME),
        "url": replit_url
    }

def run():
    # Use a different port than the one used for the socket lock in main.py
    # The socket lock uses port 10001, so we'll use a different port for Flask
    port = int(os.getenv('PORT', 10000))  # Changed default from 8080 to 10000
    
    # Always avoid using port 10001 which is used for the socket lock
    if port == 10001:
        port = 10000
        logging.info(f"Changed Flask port to {port} to avoid conflict with socket lock")
    
    try:
        app.run(
            host='0.0.0.0',
            port=port,
            debug=False,
            threaded=True  # Ensure Flask runs in threaded mode
        )
    except OSError as e:
        # Handle case where port is already in use
        if "Address already in use" in str(e):
            logging.warning(f"Port {port} already in use, trying alternate port")
            try:
                # Try an alternate port
                alt_port = port + 1
                if alt_port == 10001:  # Skip the lock port
                    alt_port += 1
                logging.info(f"Attempting to use alternate port {alt_port}")
                app.run(
                    host='0.0.0.0',
                    port=alt_port,
                    debug=False,
                    threaded=True
                )
            except Exception as inner_e:
                logging.error(f"Failed to start on alternate port: {inner_e}")
                return False
        else:
            logging.error(f"Failed to start Flask server: {e}")
            return False
    except Exception as e:
        logging.error(f"Unexpected error starting Flask server: {e}")
        return False
    
    return True

# Track start time for uptime calculation
START_TIME = datetime.now()

def self_ping():
    """Ping the application every 10 minutes to prevent sleeping"""
    while True:
        try:
            # Get the render URL from environment variable
            render_url = os.getenv('RENDER_EXTERNAL_URL')
            if render_url:
                requests.get(f"{render_url}/health")
                logging.info("Self-ping successful")
            time.sleep(600)  # Wait 10 minutes
        except Exception as e:
            logging.error(f"Self-ping failed: {e}")
            time.sleep(30)  # Wait 30 seconds before retrying

def keep_alive():
    """
    Creates and starts a Flask server in a new thread to keep the bot alive.
    Provides a web interface to monitor the bot's status.
    
    Returns:
        bool: True if the server started successfully, False otherwise
    """
    # Check if a Flask server is already running on the port
    port = int(os.getenv('PORT', 10000))
    
    # Try to detect if another server is already running on this port
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.settimeout(1)
        result = test_socket.connect_ex(('localhost', port))
        test_socket.close()
        
        if result == 0:
            logging.warning(f"Port {port} is already in use. Another server may be running.")
            # We'll still try to start our server as it might be our own previous instance
    except Exception as e:
        logging.warning(f"Error checking port availability: {e}")
    
    try:
        # Start the Flask server in a separate thread with a unique name
        # This helps identify and manage the thread
        server_thread = Thread(target=run, name="FlaskServerThread")
        server_thread.daemon = True  # Make sure thread doesn't block program exit
        server_thread.start()
        
        # Wait for the Flask server to initialize
        # This helps prevent race conditions with the bot polling
        initialization_time = 4  # Increased wait time for better stability
        logging.info(f"Waiting {initialization_time} seconds for Flask server to initialize...")
        time.sleep(initialization_time)
        
        # Check if the Flask thread is still alive after initialization
        if not server_thread.is_alive():
            logging.error("Flask server thread died during initialization")
            return False
            
        logging.info("Keep alive server started successfully")
        
        # Start the self-ping thread with a unique name to keep the service active
        ping_thread = Thread(target=self_ping, name="SelfPingThread")
        ping_thread.daemon = True
        ping_thread.start()
        logging.info("Self-ping service started")
        
        # Log the URLs for monitoring
        render_url = os.getenv('RENDER_EXTERNAL_URL', f"http://0.0.0.0:{port}")
        logging.info(f"Dashboard available at: {render_url}")
        logging.info(f"Health endpoint: {render_url}/health")
        logging.info(f"Server can be monitored at: http://0.0.0.0:{port} or your deployment URL")
    except Exception as e:
        logging.error(f"Failed to start keep alive server: {e}")
        # Log the full traceback for better debugging
        import traceback
        logging.error(traceback.format_exc())
        # Don't raise the exception, just log it and continue
        # This prevents the keep_alive failure from stopping the bot
        logging.warning("Continuing without keep alive server")
        return False
    
    return True