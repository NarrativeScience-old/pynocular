"""Configuration for engines and models"""

from ns_env_config import EnvConfig

POOL_RECYCLE = EnvConfig.integer("POOL_RECYCLE", 300)
DB_POOL_MIN_SIZE = EnvConfig.integer("DB_POOL_MIN_SIZE", 2)
DB_POOL_MAX_SIZE = EnvConfig.integer("DB_POOL_MAX_SIZE", 10)
