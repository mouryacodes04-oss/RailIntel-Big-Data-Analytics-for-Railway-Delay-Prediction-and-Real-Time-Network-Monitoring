import os


DATASET_ROOT = os.getenv("DATASET_ROOT", "Datasets")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://railway:railway_dev@localhost:5432/railway",
)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "train_positions")
