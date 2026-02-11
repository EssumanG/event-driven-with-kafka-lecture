from abc import abstractmethod, ABC
import pathlib
import csv
import random
from datetime import datetime, timedelta
import uuid
from model import UserEvent, ProductPurchaseEvent, ProductViewEvent, CUSTOMER_NAMES, PRODUCTS
from kafka import KafkaProducer
import os
import logging
import json

from generator_config import GeneratorConfig, OutputMode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
TOPIC_NAME = os.environ.get("TOPIC_NAME")


class UserEventGenerator(ABC):
    products = list(PRODUCTS.keys())
    def __init__(self, config: GeneratorConfig):
        self.events: list[UserEvent] = []
        self.fieldnames: list[str]
        self.filename = ""
        self.sub_folder = ""
        self.config = config

       
        self.producer: KafkaProducer = None
        # Kafka mode
        if self.config.output_mode == OutputMode.KAFKA:
            if not self.config.kafka_bootstrap_servers or not self.config.kafka_topic:
                raise ValueError(
                    "Kafka output_mode requires kafka_bootstrap_servers and kafka_topic"
                )
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers= self.config.kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8")
                )
                logging.info(
                    f"User Event Generator successfully subscribed to broker: {self.config.kafka_bootstrap_servers}"
                )
            except Exception as e:
                logging.error(
                    f"User Event Generator failed to subscribe to broker: {self.config.kafka_bootstrap_servers}. "
                    f"Error: {e}"
                )
                raise RuntimeError("Kafka producer initialization failed") from e
        else:
            logging.info("UserEventGenerator running in csv export mode.")

    
    @abstractmethod
    def prepare_event(self) -> list[UserEvent]:
        pass

    def do_csv_export(self):
        """Export the event into a csv file"""
        export_dir = self.config.csv_export_folder
        folder = export_dir / self.sub_folder
        folder.mkdir(exist_ok=True, parents=True)
        # file_path = folder/self.filename
        file_path = folder / f"{self.filename}_{uuid.uuid4()}.csv"
        logging.info("Exporting events to CSV directory: %s", folder)
    
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
            for event in self.events:
                writer.writerow(event.to_dict())

        logger.info("Exported events to %s", file_path)

    def produce_to_kafka(self):
        if not self.producer or not self.config.kafka_topic:
            logger.warning("Kafka producer or topic not configured; skipping produce.")
            return

        for event in self.events:
            self.producer.send(self.config.kafka_topic, event.to_dict())

        self.producer.flush()
        logger.info("Produced %d events to Kafka topic '%s'", len(self.events), self.config.kafka_topic)

    def produce_events(self):
        if self.config.output_mode == OutputMode.CSV:
            self.do_csv_export()
        else:
            self.produce_to_kafka()

class ProductViewGenerator(UserEventGenerator):
    # sub_folder = "/product-purchased-events"
    def __init__(self, config: GeneratorConfig):
        super().__init__(config)
        self.sub_folder = "product-view-events"
        self.filename = f"product_view_events_{uuid.uuid4()}.csv"
        self.fieldnames =  ['event_type', 'product_name', 'unit_price', 'customer_surname', 'customer_firstname', 'date_viewed']
    
    def prepare_event(self) -> list[UserEvent]:
        """Return list of product view by user event"""
        v_prod_name =random.choice(UserEventGenerator.products)

        today = datetime.today()
        num_of_events  = random.randint(25, 50)
        for _ in range(num_of_events):
            event = ProductViewEvent(
                product_name=v_prod_name,
                customer_firstname = random.choice(CUSTOMER_NAMES).split(' ')[1],
                customer_surname = random.choice(CUSTOMER_NAMES).split(' ')[0],
                unit_price=PRODUCTS[v_prod_name],
                date_viewed=(today - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 60))).strftime("%d-%m-%Y, %H:%M:%S"))
            self.events.append(event)

        return self.events
    
     
class ProductPurchaseGenerator(UserEventGenerator):
    def __init__(self, config: GeneratorConfig):
        super().__init__(config)
        self.sub_folder = "product-purchase-events"
        self.filename = f"product_purchase_event"
        self.fieldnames = ['event_type', 'product_name', 'customer_surname', 'customer_firstname', 'date_purchased', 'unit_price', 'quantity']
        
    def prepare_event(self) -> list[UserEvent]:
        """Return list of product purchases by user event"""

        today = datetime.today()
        num_of_events  = random.randint(25, 50)
        for _ in range(num_of_events):
            v_prod_name =random.choice(UserEventGenerator.products)

            event = ProductPurchaseEvent(
                product_name= v_prod_name,
                customer_firstname = random.choice(CUSTOMER_NAMES).split(' ')[1],
                customer_surname = random.choice(CUSTOMER_NAMES).split(' ')[0],
                unit_price=PRODUCTS[v_prod_name],
                quantity = random.randint(1, 5),
                date_purchased=(today - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 60))).strftime("%d-%m-%Y, %H:%M:%S"))
                
            self.events.append(event)
        return self.events
     
     
class GeneratorFactory(ABC):
    @abstractmethod
    def get_generators(self) -> list[UserEventGenerator]:
        pass



class ConcreteGeneratorFactory(GeneratorFactory):
    def __init__(self, config: GeneratorConfig):
        self.generators: list[UserEventGenerator] = [
            ProductViewGenerator(config),
            ProductPurchaseGenerator(config)
        ]

    def get_generators(self) -> list[UserEventGenerator]:
        return self.generators

def generate_and_export_all_events(factory: GeneratorFactory):
    generators = factory.get_generators()
    
    for generator in generators:
        generator.prepare_event()
        generator.produce_events()

