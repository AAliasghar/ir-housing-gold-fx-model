import logging
import logging.config
import yaml
from datetime import datetime

with open("config/logging.conf", "r") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)
logger.info("Ingestion utility initialized at: %s", datetime.now())
