"""Environment-driven settings. Every threshold that a reviewer might argue with
lives here rather than being sprinkled through the code as a magic number."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mqtt_host: str = "broker"
    mqtt_port: int = 1883
    mqtt_keepalive: int = 15

    # clean_session=False: the broker keeps our subscription and queues QoS-1 messages
    # while the backend is restarting, so a deploy does not lose telemetry.
    mqtt_client_id: str = "fleet-backend"
    mqtt_clean_session: bool = False

    data_dir: Path = Path("/data")
    db_path: Path = Path("/var/lib/fleet/fleet.db")
    history_enabled: bool = True
    history_flush_rows: int = 200
    history_flush_seconds: float = 1.0

    # Robots report every 5s. Two missed ticks plus a margin is "something is wrong";
    # six is "assume it is gone". Deliberately not one missed tick - a single dropped
    # packet on a cellular link is normal and should not page an operator.
    watchdog_interval_seconds: float = 2.0
    stale_after_seconds: float = 12.0
    lost_after_seconds: float = 30.0

    low_battery_pct: float = 20.0

    # How many past updates the hub keeps so a reconnecting WebSocket client can
    # resume instead of re-syncing. 500 covers ~5 minutes of an 8-robot fleet.
    hub_replay_buffer: int = 500
    hub_subscriber_queue: int = 256
    hub_heartbeat_seconds: float = 10.0


settings = Settings()
