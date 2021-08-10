"""Configuration for engines and models"""

from ns_env_config import EnvConfig

pool_recycle = EnvConfig.integer("POOL_RECYCLE", 300)
db_pool_min_size = EnvConfig.integer("TALOS_DB_POOL_MIN_SIZE", 2)
db_pool_max_size = EnvConfig.integer("TALOS_DB_POOL_MAX_SIZE", 10)
