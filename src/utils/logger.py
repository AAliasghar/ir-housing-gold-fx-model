import logging
import logging.config
import yaml
import os
from datetime import datetime

# Define the config path
config_path = "config/logging.conf"

if os.path.exists(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
else:
    # Fallback to basic logging if file is missing
    logging.basicConfig(level=logging.INFO)
    print(f"Warning: {config_path} not found, using default logging.")

logger = logging.getLogger(__name__)
logger.info("Ingestion utility initialized at: %s", datetime.now())
