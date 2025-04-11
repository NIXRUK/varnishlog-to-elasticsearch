import os

ES_BASE_URL = os.getenv("ES_BASE_URL", "https://10.99.99.4:9200")
USERNAME = os.getenv("ES_USERNAME", "elastic")
PASSWORD = os.getenv("ES_PASSWORD", "")
VERIFY_SSL = os.getenv("ES_VERIFY_SSL", "false").lower() in ["true", "1", "yes"]
