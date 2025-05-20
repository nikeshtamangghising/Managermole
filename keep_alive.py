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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    try:
        # Use gunicorn if available, otherwise fall back to Flask's development server
        try:
            import gunicorn.app.base
            from gunicorn.six import iteritems
            
            class StandaloneApplication(gunicorn.app.base.BaseApplication):
                def __init__(self, app, options=None):
                    self.options = options or {}
                    self.application = app
                    super(StandaloneApplication, self).__init__()

                def load_config(self):
                    config = dict([(key, value) for key, value in iteritems(self.options)
                                 if key in self.cfg.settings and value is not None])
                    for key, value in iteritems(config):
                        self.cfg.set(key.lower(), value)

                def load(self):
                    return self.application

            options = {
                'bind': '%s:%s' % ('0.0.0.0', '10000'),
                'workers': 1,
                'worker_class': 'sync',
                'timeout': 120,
                'keepalive': 5,
                'accesslog': '-',
                'errorlog': '-',
                'loglevel': 'info'
            }
            
            StandaloneApplication(app, options).run()
        except ImportError:
            # Fall back to Flask's development server if gunicorn is not available
            app.run(host='0.0.0.0', port=10000)
            
    except Exception as e:
        logger.error(f"Error in keep_alive server: {e}")

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
    t = Thread(target=run)
    t.daemon = True
    t.start()