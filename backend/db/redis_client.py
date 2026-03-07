import os
import redis
import logging

logger = logging.getLogger(__name__)

# Use sync redis since the existing graph builder logic is synchronous
# Parse REDIS_URL from environment
redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")

try:
    # Decode responses to get strings back instead of bytes
    redis_client = redis.from_url(redis_url, decode_responses=True)
    # Test connection
    redis_client.ping()
    logger.info("Successfully connected to Redis at %s", redis_url)
except Exception as e:
    logger.error("Failed to connect to Redis: %s", e)
    # Fallback to a dummy client that fails gracefully if redis is down
    class DummyRedis:
        def get(self, key): return None
        def setex(self, key, time, value): pass
    redis_client = DummyRedis()
