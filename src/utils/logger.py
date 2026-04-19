import logging
import yaml

with open("config/logging.conf", "r") as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)
logger.info("Ingestion started for date: %s", today)
