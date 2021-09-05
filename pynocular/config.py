"""Configuration for engines and models"""


POOL_RECYCLE = int(os.environ.get("POOL_RECYCLE", 300))
DB_POOL_MIN_SIZE = int(os.environ.get(("DB_POOL_MIN_SIZE", 2))
DB_POOL_MAX_SIZE = int(os.environ.get("DB_POOL_MAX_SIZE", 10))
