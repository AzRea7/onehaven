from pathlib import Path
import re

ROOT = Path("app")

ABSOLUTE_MAP = {
    # domain
    "app.domain.jurisdiction_defaults": "app.domain.policy.defaults",
    "app.domain.jurisdiction_categories": "app.domain.policy.categories",
    "app.domain.policy_authority": "app.domain.policy.authority",
    "app.domain.policy_decisions": "app.domain.policy.decisions",
    "app.domain.policy_conflicts": "app.domain.policy.conflicts",
    "app.domain.policy_evidence": "app.domain.policy.evidence",
    "app.domain.policy_materiality": "app.domain.policy.materiality",
    "app.domain.policy_expected_universe": "app.domain.policy.expected_universe",

    # services
    "app.services.policy_source_service": "app.services.policy_sources.source_service",
    "app.services.policy_discovery_service": "app.services.policy_sources.discovery_service",
    "app.services.policy_crawl_inventory_service": "app.services.policy_sources.crawl_inventory_service",
    "app.services.policy_fetch_service": "app.services.policy_sources.fetch_service",
    "app.services.policy_dataset_service": "app.services.policy_sources.dataset_service",
    "app.services.policy_catalog_admin_service": "app.services.policy_sources.catalog_admin_service",

    "app.services.policy_extractor_service": "app.services.policy_assertions.extractor_service",
    "app.services.policy_validation_service": "app.services.policy_assertions.validation_service",
    "app.services.policy_truth_resolution_service": "app.services.policy_assertions.truth_resolution_service",
    "app.services.policy_review_service": "app.services.policy_assertions.review_service",
    "app.services.policy_cleanup_service": "app.services.policy_assertions.cleanup_service",

    "app.services.jurisdiction_completeness_service": "app.services.policy_coverage.completeness_service",
    "app.services.jurisdiction_expected_universe_service": "app.services.policy_coverage.expected_universe_service",
    "app.services.policy_coverage_service": "app.services.policy_coverage.coverage_service",
    "app.services.jurisdiction_sla_service": "app.services.policy_coverage.sla_service",
    "app.services.jurisdiction_lockout_service": "app.services.policy_coverage.lockout_service",
    "app.services.jurisdiction_health_service": "app.services.policy_coverage.health_service",

    "app.services.jurisdiction_rules_service": "app.services.policy_governance.rules_service",
    "app.services.jurisdiction_notification_service": "app.services.policy_governance.notification_service",
    "app.services.jurisdiction_refresh_service": "app.services.policy_governance.refresh_service",

    "app.services.policy_projection_service": "app.services.compliance_engine.projection_service",
    "app.services.compliance_brief_service": "app.services.compliance_engine.brief_service",
    "app.services.policy_evidence_rollup_service": "app.services.compliance_engine.evidence_rollup_service",
    "app.services.inspection_risk_service": "app.services.compliance_engine.inspection_risk_service",
    "app.services.fix_plan_service": "app.services.compliance_engine.fix_plan_service",
    "app.services.revenue_risk_service": "app.services.compliance_engine.revenue_risk_service",
    "app.services.compliance_recommendation_service": "app.services.compliance_engine.recommendation_service",

    "app.services.inspection_template_service": "app.services.inspections.template_service",
    "app.services.inspection_readiness_service": "app.services.inspections.readiness_service",
    "app.services.nspire_import_service": "app.services.inspections.import_nspire_service",
    "app.services.inspection_failure_task_service": "app.services.inspections.failure_task_service",

    "app.services.property_inventory_snapshot_service": "app.services.properties.inventory_snapshot_service",
    "app.services.property_state_machine": "app.services.properties.state_machine",
    "app.services.property_ops_summary_service": "app.services.properties.ops_summary_service",

    "app.services.policy_pipeline_service": "app.services.policy_pipeline.pipeline_service",
}

FLAT_SERVICE_MAP = {
    "policy_source_service": "app.services.policy_sources.source_service",
    "policy_discovery_service": "app.services.policy_sources.discovery_service",
    "policy_crawl_inventory_service": "app.services.policy_sources.crawl_inventory_service",
    "policy_fetch_service": "app.services.policy_sources.fetch_service",
    "policy_dataset_service": "app.services.policy_sources.dataset_service",
    "policy_catalog_admin_service": "app.services.policy_sources.catalog_admin_service",

    "policy_extractor_service": "app.services.policy_assertions.extractor_service",
    "policy_validation_service": "app.services.policy_assertions.validation_service",
    "policy_truth_resolution_service": "app.services.policy_assertions.truth_resolution_service",
    "policy_review_service": "app.services.policy_assertions.review_service",
    "policy_cleanup_service": "app.services.policy_assertions.cleanup_service",

    "jurisdiction_completeness_service": "app.services.policy_coverage.completeness_service",
    "jurisdiction_expected_universe_service": "app.services.policy_coverage.expected_universe_service",
    "policy_coverage_service": "app.services.policy_coverage.coverage_service",
    "jurisdiction_sla_service": "app.services.policy_coverage.sla_service",
    "jurisdiction_lockout_service": "app.services.policy_coverage.lockout_service",
    "jurisdiction_health_service": "app.services.policy_coverage.health_service",

    "jurisdiction_rules_service": "app.services.policy_governance.rules_service",
    "jurisdiction_notification_service": "app.services.policy_governance.notification_service",
    "jurisdiction_refresh_service": "app.services.policy_governance.refresh_service",

    "policy_projection_service": "app.services.compliance_engine.projection_service",
    "compliance_brief_service": "app.services.compliance_engine.brief_service",
    "policy_evidence_rollup_service": "app.services.compliance_engine.evidence_rollup_service",
    "inspection_risk_service": "app.services.compliance_engine.inspection_risk_service",
    "fix_plan_service": "app.services.compliance_engine.fix_plan_service",
    "revenue_risk_service": "app.services.compliance_engine.revenue_risk_service",
    "compliance_recommendation_service": "app.services.compliance_engine.recommendation_service",

    "inspection_template_service": "app.services.inspections.template_service",
    "inspection_readiness_service": "app.services.inspections.readiness_service",
    "nspire_import_service": "app.services.inspections.import_nspire_service",
    "inspection_failure_task_service": "app.services.inspections.failure_task_service",

    "property_inventory_snapshot_service": "app.services.properties.inventory_snapshot_service",
    "property_state_machine": "app.services.properties.state_machine",
    "property_ops_summary_service": "app.services.properties.ops_summary_service",

    "policy_pipeline_service": "app.services.policy_pipeline.pipeline_service",

    # top-level services that stayed top-level
    "jurisdiction_profile_service": "app.services.jurisdiction_profile_service",
    "market_sync_service": "app.services.market_sync_service",
    "ingestion_scheduler_service": "app.services.ingestion_scheduler_service",
    "compliance_service": "app.services.compliance_service",
    "compliance_document_service": "app.services.compliance_document_service",
    "jurisdiction_task_mapper": "app.services.jurisdiction_task_mapper",
    "risk_scoring": "app.services.risk_scoring",  
}

FLAT_DOMAIN_MAP = {
    "jurisdiction_defaults": "app.domain.policy.defaults",
    "jurisdiction_categories": "app.domain.policy.categories",
    "policy_authority": "app.domain.policy.authority",
    "policy_decisions": "app.domain.policy.decisions",
    "policy_conflicts": "app.domain.policy.conflicts",
    "policy_evidence": "app.domain.policy.evidence",
    "policy_materiality": "app.domain.policy.materiality",
    "policy_expected_universe": "app.domain.policy.expected_universe",
}

MOVED_SERVICE_DIRS = {
    "app/services/policy_coverage",
    "app/services/policy_governance",
    "app/services/policy_sources",
    "app/services/policy_assertions",
    "app/services/compliance_engine",
    "app/services/inspections",
    "app/services/properties",
    "app/services/policy_pipeline",
}

PY_FILES = [p for p in ROOT.rglob("*.py") if "__pycache__" not in p.parts]

def is_in_moved_service_dir(path: Path) -> bool:
    s = path.as_posix()
    return any(s.startswith(prefix) for prefix in MOVED_SERVICE_DIRS)

def fix_absolute(text: str) -> str:
    for old, new in sorted(ABSOLUTE_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        text = text.replace(old, new)

    bad_prefixes = {
        "from app.policy_governance.": "from app.services.policy_governance.",
        "from app.policy_coverage.": "from app.services.policy_coverage.",
        "from app.policy_assertions.": "from app.services.policy_assertions.",
        "from app.policy_sources.": "from app.services.policy_sources.",
        "from app.compliance_engine.": "from app.services.compliance_engine.",
        "from app.inspections.": "from app.services.inspections.",
        "from app.properties.": "from app.services.properties.",
        "from app.policy_pipeline.": "from app.services.policy_pipeline.",
        "import app.policy_governance.": "import app.services.policy_governance.",
        "import app.policy_coverage.": "import app.services.policy_coverage.",
        "import app.policy_assertions.": "import app.services.policy_assertions.",
        "import app.policy_sources.": "import app.services.policy_sources.",
        "import app.compliance_engine.": "import app.services.compliance_engine.",
        "import app.inspections.": "import app.services.inspections.",
        "import app.properties.": "import app.services.properties.",
        "import app.policy_pipeline.": "import app.services.policy_pipeline.",
    }
    for old, new in bad_prefixes.items():
        text = text.replace(old, new)

    return text

def rewrite_from_line(line: str, path: Path) -> str:
    m = re.match(r"^(\s*)from\s+([\.]+)([A-Za-z0-9_\.]+)\s+import\s+(.+)$", line)
    if not m:
        return line

    indent, dots, module_name, imports = m.groups()

    if module_name in FLAT_SERVICE_MAP:
        return f"{indent}from {FLAT_SERVICE_MAP[module_name]} import {imports}"
    
    if module_name == "config":
        return f"{indent}from app.config import {imports}"

    if module_name == "db":
        return f"{indent}from app.db import {imports}"

    if module_name == "schemas":
        return f"{indent}from app.schemas import {imports}"
    
    if module_name in FLAT_DOMAIN_MAP:
        return f"{indent}from {FLAT_DOMAIN_MAP[module_name]} import {imports}"

    if module_name.startswith("domain."):
        suffix = module_name[len("domain."):]
        if suffix in FLAT_DOMAIN_MAP:
            return f"{indent}from {FLAT_DOMAIN_MAP[suffix]} import {imports}"
        return f"{indent}from app.domain.{suffix} import {imports}"

    if module_name.startswith("services."):
        suffix = module_name[len("services."):]
        if suffix in FLAT_SERVICE_MAP:
            return f"{indent}from {FLAT_SERVICE_MAP[suffix]} import {imports}"
        return f"{indent}from app.services.{suffix} import {imports}"

    if module_name == "models":
        return f"{indent}from app.models import {imports}"
    
    if module_name.startswith("config."):
        suffix = module_name[len("config."):]
        return f"{indent}from app.config.{suffix} import {imports}"

    if module_name.startswith("db."):
        suffix = module_name[len("db."):]
        return f"{indent}from app.db.{suffix} import {imports}"

    if module_name.startswith("schemas."):
        suffix = module_name[len("schemas."):]
        return f"{indent}from app.schemas.{suffix} import {imports}"
    if module_name == "policy_models":
        return f"{indent}from app.policy_models import {imports}"

    # critical new case:
    # inside moved service folders, "from .foo_service import ..." should target top-level app.services.foo_service
        # inside moved service folders, same-folder relative imports often need to
    # point back to top-level app.services.<module>
    if is_in_moved_service_dir(path):
        if module_name in FLAT_SERVICE_MAP:
            return f"{indent}from {FLAT_SERVICE_MAP[module_name]} import {imports}"

        # generic fallback for top-level helper modules that stayed in app/services
        return f"{indent}from app.services.{module_name} import {imports}"

    return line

def rewrite_import_line(line: str) -> str:
    for old, new in sorted(ABSOLUTE_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        line = re.sub(rf"(\bimport\s+){re.escape(old)}(\b)", rf"\1{new}\2", line)
    return line

def rewrite_file(path: Path) -> bool:
    original = path.read_text(encoding="utf-8")
    text = fix_absolute(original)

    lines = text.splitlines()
    new_lines = []
    for line in lines:
        line = rewrite_from_line(line, path)
        line = rewrite_import_line(line)
        new_lines.append(line)

    updated = "\n".join(new_lines)
    if original.endswith("\n"):
        updated += "\n"

    if updated != original:
        path.write_text(updated, encoding="utf-8")
        print(f"updated: {path}")
        return True
    return False

print(f"cwd={Path.cwd()}")
print(f"root_exists={ROOT.exists()}")
print(f"python_files={len(PY_FILES)}")

changed = 0
for path in PY_FILES:
    if rewrite_file(path):
        changed += 1

print(f"done. changed {changed} files.")