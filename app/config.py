from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TS_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://tail:tail@localhost:5432/tailsampling"
    window_ms: int = 30_000
    hard_watermark_ms: int = 60_000
    sweep_interval_ms: int = 1_000
    enable_sweeper: bool = True
    seed_default_policy: bool = True
    # Test-only fault-injection endpoints (/admin/test/*). Never enabled in
    # the shipped Docker image / compose file.
    enable_test_hooks: bool = False
    # How long a PENDING idempotency placeholder may stay owned before another
    # request is allowed to take it over (crash recovery). A placeholder is
    # always released by commit/rollback while the owning process is alive, so
    # this only matters for hard crashes / lost connections.
    idempotency_pending_ttl_ms: int = 60_000

    @property
    def window_seconds(self) -> float:
        return self.window_ms / 1000.0

    @property
    def hard_watermark_seconds(self) -> float:
        return self.hard_watermark_ms / 1000.0

    @property
    def sweep_interval_seconds(self) -> float:
        return self.sweep_interval_ms / 1000.0

    @property
    def idempotency_pending_ttl_seconds(self) -> float:
        return self.idempotency_pending_ttl_ms / 1000.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
