from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---- App ----
    app_env: str = "local"
    database_url: str = "sqlite:///./onehaven.db"

    # ---- Operating Truth / Reproducibility ----
    payment_standard_pct: float = 1.10
    decision_version: str = "2026-02-10.v1"

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

    # ---- Rent calibration (learning loop) ----
    rent_calibration_alpha: float = 0.20

    # back-compat (typo) – keep reading it if someone set it in env by accident
    rent_calibration_apha: float | None = None

    rent_calibration_min_mult: float = 0.70
    rent_calibration_max_mult: float = 1.30
    default_payment_standard_pct: float = 1.10

    # ---- External APIs ----
    hud_user_token: str | None = None
    hud_base_url: str = "https://www.huduser.gov/hudapi/public"

    rentcast_api_key: str | None = None
    rentcast_base_url: str = "https://api.rentcast.io/v1"

    # ---- SaaS Auth (cookie JWT) ----
    # Your backend /auth routes already set + read a JWT cookie.
    jwt_secret: str = "dev-change-me"  # override in .env in real envs
    jwt_exp_minutes: int = 60 * 24 * 7  # 7 days
    jwt_cookie_name: str = "onehaven_jwt"
    jwt_cookie_secure: int = 0  # set 1 in prod over HTTPS
    jwt_cookie_samesite: str = "lax"  # lax|strict|none

    # ---- Phase 5: Tenancy helpers (dev) ----
    dev_auto_verify_email: bool = True

    # Local-dev bypass (ONLY honored when app_env == "local")
    # Use headers:
    #   X-User-Email: you@domain.com
    #   X-Org-Slug: onehaven (optional)
    #   X-User-Role: owner
    allow_local_auth_bypass: bool = True
    dev_header_org_slug: str = "X-Org-Slug"
    dev_header_user_email: str = "X-User-Email"
    dev_header_user_role: str = "X-User-Role"

    celery_broker_url: str | None = None
    celery_result_backend: str | None = None

    # ---- Agent runtime limits ----
    agents_max_runs_per_property_per_hour: int = 3
    agents_max_retries: int = 3
    agents_run_timeout_seconds: int = 120

    def model_post_init(self, __context) -> None:
        if self.rent_calibration_apha is not None:
            object.__setattr__(self, "rent_calibration_alpha", float(self.rent_calibration_apha))


settings = Settings()
