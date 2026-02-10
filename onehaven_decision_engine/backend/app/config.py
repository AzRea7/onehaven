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

    min_inventory: int | None = None

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

    # ✅ Correct spelling
    rent_calibration_alpha: float = 0.20

    # ✅ Back-compat for old env var / old field name (deprecated)
    rent_calibration_apha: float | None = None

    rent_calibration_min_mult: float = 0.70
    rent_calibration_max_mult: float = 1.30

    # Section 8 payment standard (PHA policy proxy)
    default_payment_standard_pct: float = 1.00

    # ---- External APIs ----
    hud_user_token: str | None = None
    hud_base_url: str = "https://www.huduser.gov/hudapi/public"

    rentcast_api_key: str | None = None
    rentcast_base_url: str = "https://api.rentcast.io/v1"

    def model_post_init(self, __context) -> None:
        # If someone used the misspelled config, keep the app running.
        if self.rent_calibration_apha is not None:
            object.__setattr__(self, "rent_calibration_alpha", float(self.rent_calibration_apha))


settings = Settings()
