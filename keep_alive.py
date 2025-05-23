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
    <title>Bot Status Dashboard - {{ platform }}</title>
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
        <h1>🤖 Bot Status Dashboard - {{ platform }}</h1>
        
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
                <p>Service URL: <span id="service-url">{{ service_url }}</span></p>
                <p style="color: #1557b0; font-size: 0.9em;">⚡ This is your permanent URL for UptimeRobot</p>
                <button onclick="copyUrl()" class="refresh" style="background-color: #34a853;">📋 Copy URL</button>
                <script>
                    function copyUrl() {
                        const url = document.getElementById('service-url').textContent;
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
    # Check if running on Render
    is_render = os.environ.get('RENDER', '') == 'true'
    
    # Get the appropriate URL based on environment
    if is_render:
        service_url = os.environ.get('RENDER_EXTERNAL_URL', 'https://your-app-name.onrender.com')
        platform_name = "Render"
    else:
        service_url = f'https://{os.getenv("REPL_SLUG", "unknown")}.{os.getenv("REPL_OWNER", "unknown")}.replit.co'
        platform_name = "Replit"
    
    stats = {
        'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'cpu_usage': round(psutil.cpu_percent(), 1),
        'memory_usage': round(psutil.virtual_memory().percent, 1),
        'uptime': str(datetime.now() - START_TIME).split('.')[0],
        'port': os.getenv('PORT', 8080),
        'host': '0.0.0.0',
        'debug': 'Disabled',
        'service_url': service_url,
        'platform': platform_name
    }
    return render_template_string(HOME_TEMPLATE, **stats)

@app.route('/health')
def health():
    # Check if running on Render
    is_render = os.environ.get('RENDER', '') == 'true'
    
    # Get the appropriate URL based on environment
    if is_render:
        service_url = os.environ.get('RENDER_EXTERNAL_URL', 'https://your-app-name.onrender.com')
    else:
        service_url = f'https://{os.getenv("REPL_SLUG", "unknown")}.{os.getenv("REPL_OWNER", "unknown")}.replit.co'
    
    return {
        "status": "healthy",
        "message": "Bot is running",
        "timestamp": datetime.now().isoformat(),
        "uptime": str(datetime.now() - START_TIME),
        "url": service_url,
        "platform": "Render" if is_render else "Replit"
    }

def run():
    """Run the Flask server for keep-alive purposes."""
    # Get port from environment variable on Render.com
    port = int(os.getenv('PORT', 10000))
    
    # Check if we're running on Render.com
    is_render = os.environ.get('RENDER', '') == 'true'
    if is_render:
        logging.info(f"Running Flask server on Render.com with PORT={port}")
    
    # Always avoid using port 10001 which is used for the socket lock
    if port == 10001:
        port = 10000
        logging.info(f"Changed Flask port to {port} to avoid conflict with socket lock")
    
    # For Render.com, we need to bind to the PORT environment variable
    host = '0.0.0.0'  # Use 0.0.0.0 to bind to all interfaces
    
    try:
        app.run(
            host=host,
            port=port,
            debug=False,
            threaded=True  # Ensure Flask runs in threaded mode
        )
        logging.info(f"Flask server running on {host}:{port}")
        return True
    except OSError as e:
        # Handle case where port is already in use
        if "Address already in use" in str(e):
            logging.warning(f"Port {port} already in use, trying alternate port")
            try:
                # Try an alternate port - but if we're on Render, use their PORT
                alt_port = port + 2  # Skip port+1 which might be used by another service
                if alt_port == 10001:  # Skip the lock port
                    alt_port += 1
                
                # If we're on Render.com, we must use their PORT and fail otherwise
                if is_render:
                    logging.critical(f"Failed to bind to Render.com PORT {port}. Service will not work correctly!")
                    # Try another port anyway as a last resort
                
                logging.info(f"Attempting to use alternate port {alt_port}")
                app.run(
                    host=host,
                    port=alt_port,
                    debug=False,
                    threaded=True
                )
                logging.info(f"Flask server running on {host}:{alt_port}")
                return True
            except Exception as inner_e:
                logging.error(f"Failed to start on alternate port: {inner_e}")
                return False
        else:
            logging.error(f"Failed to start Flask server: {e}")
            return False
    except Exception as e:
        logging.error(f"Unexpected error starting Flask server: {e}")
        return False

# Track start time for uptime calculation
START_TIME = datetime.now()

def self_ping():
    """Ping the application every 5 minutes to prevent sleeping on Render.com with smart backoff"""
    # Initial backoff parameters
    base_wait = 300  # 5 minutes base interval
    backoff_factor = 2  # Double the wait time on consecutive failures
    max_wait = 900  # Maximum 15 minutes between pings
    min_wait = 60   # Minimum 1 minute between pings
    
    consecutive_failures = 0
    last_success_time = time.time()
    
    while True:
        try:
            # Get the Render URL from environment variable, if available
            render_url = os.getenv('RENDER_EXTERNAL_URL')
            ping_successful = False
            
            if render_url:
                try:
                    # Use a HEAD request which is lighter than GET
                    response = requests.head(f"{render_url}/health", timeout=10)
                    if response.status_code == 200:
                        logging.info(f"Self-ping successful: {render_url}/health")
                        ping_successful = True
                        consecutive_failures = 0  # Reset failure count
                        last_success_time = time.time()
                    elif response.status_code >= 500:
                        # Server error - might be restarting
                        logging.warning(f"Self-ping received server error: {response.status_code}")
                        consecutive_failures += 1
                    else:
                        logging.warning(f"Self-ping received unexpected status: {response.status_code}")
                        consecutive_failures += 1
                except Exception as render_e:
                    logging.warning(f"External self-ping to Render failed: {render_e}")
                    consecutive_failures += 1
            
            # Try localhost if external ping failed or wasn't available
            if not ping_successful:
                try:
                    port = int(os.getenv('PORT', 10000))
                    # Use HEAD request here too
                    local_response = requests.head(f"http://localhost:{port}/health", timeout=5)
                    if local_response.status_code == 200:
                        logging.info("Local self-ping successful")
                        ping_successful = True
                        consecutive_failures = 0  # Reset failure count
                        last_success_time = time.time()
                    else:
                        logging.warning(f"Local self-ping got status: {local_response.status_code}")
                except Exception as local_e:
                    logging.warning(f"Local self-ping failed: {local_e}")
                    consecutive_failures += 1
                    
                # If both external and local pings failed, try an alternative local address
                if not ping_successful:
                    try:
                        alt_port = 10000 if int(os.getenv('PORT', 10000)) != 10000 else 10002
                        alt_response = requests.head(f"http://localhost:{alt_port}/health", timeout=5)
                        if alt_response.status_code == 200:
                            logging.info(f"Alternative local self-ping successful on port {alt_port}")
                            ping_successful = True
                            consecutive_failures = 0
                            last_success_time = time.time()
                    except:
                        # Just ignore errors on this last-ditch attempt
                        pass
            
            # Calculate next wait time using exponential backoff
            if consecutive_failures > 0:
                # Calculate backoff with a cap
                wait_time = min(base_wait * (backoff_factor ** (consecutive_failures - 1)), max_wait)
                logging.info(f"Using backoff wait of {wait_time}s after {consecutive_failures} failures")
            else:
                wait_time = base_wait
                
            # But if it's been too long since last success, use the minimum wait
            if time.time() - last_success_time > 600:  # 10 minutes
                wait_time = min_wait
                logging.warning(f"No successful ping for 10+ minutes, using minimum wait: {min_wait}s")
                
            time.sleep(wait_time)
            
        except Exception as e:
            logging.error(f"Self-ping error: {e}")
            time.sleep(min_wait)  # Use minimum wait on exceptions

def keep_alive():
    """Start a Flask server to keep the bot alive and initialize self-ping.
    
    Returns:
        bool: True if the server started successfully, False otherwise
    """
    try:
        # On Render.com, we need to use the PORT assigned by the platform
        port_to_check = int(os.getenv('PORT', 10000))
        
        # Check if the port is already in use
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)  # 2 second timeout
        result = sock.connect_ex(('localhost', port_to_check))
        sock.close()
        
        if result == 0:  # Port is already in use
            logging.warning(f"Port {port_to_check} is already in use - another instance may be running")
            logging.info("Using existing keep-alive server")
            
            # We'll still start the ping service to ensure the app keeps running
            ping_thread = Thread(target=self_ping)
            ping_thread.daemon = True
            ping_thread.start()
            logging.info("Self-ping service started (using existing server)")
            
            return True
            
        # Start the Flask server in a separate thread
        server_thread = Thread(target=run)
        server_thread.daemon = True  # Thread will close when the main program exits
        server_thread.start()
        
        # Start the self-pinger in a separate thread
        ping_thread = Thread(target=self_ping)
        ping_thread.daemon = True
        ping_thread.start()
        logging.info("Self-ping service started")
        
        # Wait a moment to ensure the server starts properly
        time.sleep(1)
        logging.info("Keep-alive server started successfully")
        return True
    except Exception as e:
        logging.error(f"Failed to start keep-alive server: {e}", exc_info=True)
        # Try to at least start the self-ping service
        try:
            ping_thread = Thread(target=self_ping)
            ping_thread.daemon = True
            ping_thread.start()
            logging.info("Self-ping service started despite server error")
        except:
            pass
        return False