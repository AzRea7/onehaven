from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Loaded from environment variables.

    In Docker: values come from docker-compose.yml env_file/environment.
    Locally (non-docker): you can also create a project-root .env and run uvicorn normally.
    """

    # Try to load a local .env when running outside Docker; Docker env injection still wins.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Core
    app_env: str = "local"
    database_url: str

    # External APIs (your ask)
    hud_user_token: str = ""        # env: HUD_USER_TOKEN
    rentcast_api_key: str = ""      # env: RENTCAST_API_KEY

    # Deal rules defaults (your notes)
    max_price: int = 150_000
    min_bedrooms: int = 2
    min_inventory: int = 80
    rent_rule_min_pct: float = 0.013   # 1.3%
    rent_rule_target_pct: float = 0.015  # 1.5%

    # Underwriting defaults (your notes)
    vacancy_rate: float = 0.05
    maintenance_rate: float = 0.10
    management_rate: float = 0.08
    capex_rate: float = 0.05
    insurance_monthly: float = 150.0
    taxes_monthly: float = 300.0
    utilities_monthly: float = 0.0

    target_monthly_cashflow: float = 400.0
    target_roi: float = 0.15

    dscr_min: float = 1.20
    dscr_penalty_enabled: bool = True


settings = Settings()
