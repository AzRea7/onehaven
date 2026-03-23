# backend/app/config.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---- App ----
    app_env: str = "local"  # local|dev|prod
    database_url: str = "sqlite:///./onehaven.db"

    # ---- CORS ----
    cors_allow_origins: list[str] | str = ["*"]

    # ---- Operating Truth / Reproducibility ----
    payment_standard_pct: float = 1.10
    decision_version: str = "2026-02-10.v1"

    # ---- Deal rules defaults ----
    max_price: int = 150_000
    min_bedrooms: int = 2
    min_inventory: int | None = None

    rent_rule_min_pct: float = 0.013
    rent_rule_target_pct: float = 0.015

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

    # ---- Rent calibration ----
    rent_calibration_alpha: float = 0.20
    rent_calibration_apha: float | None = None  # back-compat typo
    rent_calibration_min_mult: float = 0.70
    rent_calibration_max_mult: float = 1.30
    default_payment_standard_pct: float = 1.10

    # ---- External APIs ----
    hud_user_token: str | None = None
    hud_base_url: str = "https://www.huduser.gov/hudapi/public"

    rentcast_api_key: str | None = None
    rentcast_base_url: str = "https://api.rentcast.io/v1"
    rentcast_daily_limit: int = 100

    # ---- Geocoding / location automation ----
    geocoding_enabled: bool = True
    geocode_default_country_code: str = "US"

    # comma-separated priority order; keep simple for env usage
    geocode_provider_order: str = "google,nominatim"

    geocode_cache_enabled: bool = True
    geocode_cache_ttl_hours: int = 24 * 30
    geocode_stale_after_hours: int = 24 * 14

    geocode_timeout_seconds: int = 12
    geocode_min_confidence: float = 0.55
    geocode_allow_fallback_providers: bool = True
    geocode_fail_open: bool = True

    geocode_refresh_on_ingestion: bool = True
    geocode_refresh_missing_only: bool = False

    location_refresh_batch_size: int = 250
    location_refresh_max_attempts: int = 3
    location_refresh_schedule_minutes: int = 12 * 60

        # ---- Ingestion cost controls ----
    ingestion_enable_inline_rent_refresh: bool = False
    ingestion_publish_without_rent: bool = True
    ingestion_queue_rent_refresh_after_sync: bool = True

    # Small burst after each sync run for the best candidates only
    ingestion_post_sync_rent_budget: int = 5

    # Daily capped rent refresh budget
    ingestion_daily_rent_refresh_limit: int = 25

    # Only rent-refresh properties missing rent or stale
    ingestion_rent_refresh_stale_after_hours: int = 24 * 7

    # Optional quality gates before a property is shown in investor inventory
    investor_require_address: bool = True
    investor_require_price: bool = True
    investor_require_geo: bool = False

        # ---- Supported market sync / scalable regional coverage ----
    # Phase 1: Southeast Michigan only
    # Phase 2: Michigan statewide
    # Phase 3: multi-state hot/warm/cold coverage
    market_sync_daily_market_limit: int = 6
    market_sync_default_limit_per_market: int = 250

    # Use "all" for now. Later:
    # - hot: sync only most important markets frequently
    # - warm: less frequent
    # - cold: rare backfill / on-demand expansion
    market_sync_daily_tier_filter: str = "all"

    # Future scalability toggle:
    # when you add a DB-backed market catalog, this can switch lookup mode
    market_catalog_backend: str = "static"

    # Future cost controls:
    # add per-market API budgets later instead of one global setting
    market_sync_enable_statewide_expansion: bool = False

    # ---- Google Geocoding ----
    google_geocode_api_key: str | None = None
    google_geocode_base_url: str = "https://maps.googleapis.com/maps/api/geocode/json"

    # ---- Nominatim ----
    nominatim_base_url: str = "https://nominatim.openstreetmap.org"
    nominatim_user_agent: str = "onehaven-location-automation/1.0"
    nominatim_email: str | None = None

    # ---- OpenCage (optional secondary provider) ----
    opencage_api_key: str | None = None
    opencage_base_url: str = "https://api.opencagedata.com/geocode/v1/json"

    # ---- SaaS Auth / tenancy ----
    auth_mode: str = "dev"  # dev|jwt
    dev_auto_provision: bool = True
    dev_auto_verify_email: bool = True
    allow_local_auth_bypass: bool = False

    dev_header_org_slug: str = "X-Org-Slug"
    dev_header_user_email: str = "X-User-Email"
    dev_header_user_role: str = "X-User-Role"

    # ---- API keys ----
    enable_api_keys: bool = False
    api_key_pepper: str = "dev-pepper-change-me"
    api_key_prefix_len: int = 10

    # ---- Plans / billing ----
    default_plan_code: str = "free"

    # ---- JWT cookie ----
    jwt_secret: str = "dev-change-me"
    jwt_exp_minutes: int = 60 * 24 * 7
    jwt_cookie_name: str = "onehaven_jwt"
    jwt_cookie_secure: int = 0
    jwt_cookie_samesite: str = "lax"

    # ---- Celery ----
    celery_broker_url: str | None = None
    celery_result_backend: str | None = None

    # ---- Agent runtime limits ----
    agents_max_runs_per_property_per_hour: int = 6
    agents_max_retries: int = 3
    agents_run_timeout_seconds: int = 180
    agents_max_running_per_org: int = 3
    agents_enable_org_concurrency_guard: bool = True
    agents_enable_pg_advisory_locks: bool = True

    # ---- Agent orchestration toggles ----
    agents_enable_auto_planning: bool = True
    agents_enable_followup_fanout: bool = True
    agents_enable_ops_judge: bool = True
    agents_enable_trust_recompute: bool = True
    agents_enable_photo_rehab: bool = True

    # ---- LM Studio / local LLM ----
    llm_provider: str = "lm_studio"  # lm_studio|disabled
    lm_studio_enabled: bool = True
    lm_studio_base_url: str = "http://127.0.0.1:1234/v1"
    lm_studio_api_key: str = "lm-studio"
    lm_studio_model: str = "qwen3-coder-30b-a3b-instruct"
    lm_studio_vision_model: str = "qwen2.5-vl-7b-instruct"
    lm_studio_timeout_seconds: int = 120
    lm_studio_temperature: float = 0.2
    lm_studio_max_tokens: int = 2048
    lm_studio_json_mode: bool = True

    # ---- Trace / observability ----
    trace_mirror_to_messages: int = 0

    @property
    def geocode_provider_order_list(self) -> list[str]:
        raw = self.geocode_provider_order or ""
        parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
        deduped: list[str] = []
        for p in parts:
            if p not in deduped:
                deduped.append(p)
        return deduped or ["nominatim"]

    def model_post_init(self, __context) -> None:
        if self.rent_calibration_apha is not None:
            object.__setattr__(self, "rent_calibration_alpha", float(self.rent_calibration_apha))

        env = (self.app_env or "local").strip().lower()
        is_prod = env in ("prod", "production")

        if is_prod:
            if (self.auth_mode or "").strip().lower() == "dev":
                raise ValueError("SECURITY: auth_mode=dev is not allowed in prod")
            if bool(self.allow_local_auth_bypass):
                raise ValueError("SECURITY: allow_local_auth_bypass=True is not allowed in prod")

            origins = self.cors_allow_origins
            if origins == "*" or origins == ["*"] or (isinstance(origins, str) and "*" in origins):
                raise ValueError("SECURITY: cors_allow_origins wildcard is not allowed in prod")

            if not self.jwt_secret or self.jwt_secret == "dev-change-me":
                raise ValueError("SECURITY: jwt_secret must be set in prod")

            if self.lm_studio_enabled and not self.lm_studio_base_url:
                raise ValueError("SECURITY: lm_studio_base_url must be configured when LM Studio is enabled")

            if self.geocoding_enabled:
                has_google = bool((self.google_geocode_api_key or "").strip())
                has_nominatim = "nominatim" in self.geocode_provider_order_list
                has_opencage = bool((self.opencage_api_key or "").strip())
                if not (has_google or has_nominatim or has_opencage):
                    raise ValueError(
                        "SECURITY: geocoding_enabled=True but no usable geocode provider is configured"
                    )

        # local/dev safety defaults
        if not getattr(self, "agents_max_running_per_org", None):
            object.__setattr__(self, "agents_max_running_per_org", 3)

        if self.agents_run_timeout_seconds < 30:
            object.__setattr__(self, "agents_run_timeout_seconds", 30)

        if self.agents_max_runs_per_property_per_hour < 1:
            object.__setattr__(self, "agents_max_runs_per_property_per_hour", 1)

        if self.geocode_timeout_seconds < 3:
            object.__setattr__(self, "geocode_timeout_seconds", 3)

        if self.geocode_cache_ttl_hours < 1:
            object.__setattr__(self, "geocode_cache_ttl_hours", 1)

        if self.geocode_stale_after_hours < 1:
            object.__setattr__(self, "geocode_stale_after_hours", 1)

        if self.location_refresh_batch_size < 1:
            object.__setattr__(self, "location_refresh_batch_size", 1)

        if self.location_refresh_max_attempts < 1:
            object.__setattr__(self, "location_refresh_max_attempts", 1)

        clamped_confidence = min(max(float(self.geocode_min_confidence), 0.0), 1.0)
        object.__setattr__(self, "geocode_min_confidence", clamped_confidence)


settings = Settings()