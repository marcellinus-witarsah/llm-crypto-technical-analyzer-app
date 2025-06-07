import logging

logger = logging.getLogger("ai-agent-crypto-analyzer")
logger.setLevel(logging.INFO)  # Set the logging level

# Add a console handler (prints logs to stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Ensure INFO messages are logged

# Set the output format: "(time) - (log level) - (log message)"
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
