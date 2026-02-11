from generator_factory import ConcreteGeneratorFactory, generate_and_export_all_events
from generator_config import OutputMode, GeneratorConfig
import pathlib
import time
import random
import os
from dotenv import load_dotenv
import logging

load_dotenv()

LOG_FORMAT = (
    "[%(asctime)s]:-%(levelname)-8s-| %(name)s | %(message)s"
)

logging.basicConfig(
      level=logging.INFO,  
      format=LOG_FORMAT,
      datefmt="%Y-%m-%d %H:%M:%S",
      force=True
    )
logger = logging.getLogger(__name__)

KAFKA_BROKER_LOCALHOST = os.environ.get("KAFKA_BROKER_LOCALHOST")
KAFKA_TOPIC_NAME = os.environ.get("TOPIC_NAME")
if __name__ == "__main__":
    csv_config = GeneratorConfig(
        output_mode=OutputMode.CSV,
        csv_export_folder=pathlib.Path("../../e-commerce-user-events")
    )
    kafka_config = GeneratorConfig(
        output_mode=OutputMode.KAFKA,
        kafka_bootstrap_servers=KAFKA_BROKER_LOCALHOST,
        kafka_topic=KAFKA_TOPIC_NAME
    )
    factory = ConcreteGeneratorFactory(kafka_config)
try:
    while True:        
        generate_and_export_all_events(factory)

        time.sleep(float(random.uniform(0, 4)))
   
except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received. Shutting down gracefully...")

finally:
        # optional cleanup if needed
        logger.info("Generator stopped.")