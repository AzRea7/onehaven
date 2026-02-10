from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # This makes local dev sane: put secrets in backend/.env and docker env_file
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "local"
    database_url: str

    # ---- Deal rules defaults ----
    max_price: int = 150_000
    min_bedrooms: int = 2
    min_inventory: int = 80
    rent_rule_min_pct: float = 0.013  # 1.3%
    rent_rule_target_pct: float = 0.015  # 1.5%

    # ---- Underwriting defaults ----
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

    # ---- External APIs ----
    # HUD User API: you generate an access token on huduser.gov and pass it here
    hud_user_token: str | None = None
    hud_base_url: str = "https://www.huduser.gov/hudapi/public"

    # RentCast API
    rentcast_api_key: str | None = None
    rentcast_base_url: str = "https://api.rentcast.io/v1"


settings = Settings()
