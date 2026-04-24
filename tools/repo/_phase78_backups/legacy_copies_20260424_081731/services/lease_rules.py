from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Any

from sqlalchemy import select, and_, or_
from sqlalchemy.orm import Session

from app.models import Lease


def _as_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    # allow ISO strings defensively
    try:
        return date.fromisoformat(str(v))
    except Exception:
        return None


def _overlaps(a_start: date, a_end: Optional[date], b_start: date, b_end: Optional[date]) -> bool:
    """
    Overlap rule:
    - Treat end dates as inclusive.
    - If end is None, treat it as open-ended.
    """
    # open-ended becomes "infinite"
    a_end_eff = a_end or date.max
    b_end_eff = b_end or date.max
    return not (a_end_eff < b_start or b_end_eff < a_start)


@dataclass(frozen=True)
class LeaseOverlapResult:
    ok: bool
    conflict_lease_id: Optional[int] = None
    message: Optional[str] = None


def ensure_no_lease_overlap(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    start_date: Any,
    end_date: Any = None,
    unit_id: Optional[int] = None,
    unit_name: Optional[str] = None,
    ignore_lease_id: Optional[int] = None,
) -> None:
    """
    Raise ValueError if an overlapping lease exists.

    Supports multiple schema styles:
    - Some repos use Lease.unit_id (int FK)
    - Others use Lease.unit_name or unit_label (string)
    - Some have only property_id (single-unit assumption)

    Tenants router imports this symbol; keep signature stable.
    """
    s = _as_date(start_date)
    e = _as_date(end_date)

    if s is None:
        raise ValueError("lease start_date is required and must be a date")

    if e is not None and e < s:
        raise ValueError("lease end_date cannot be before start_date")

    # Base query: same org + property
    q = select(Lease).where(
        Lease.org_id == int(org_id),
        Lease.property_id == int(property_id),
    )

    # Optional: unit scoping if those columns exist
    # We can't reliably introspect SQLAlchemy model columns without importing internals,
    # so we defensively check attributes.
    has_unit_id = hasattr(Lease, "unit_id")
    has_unit_name = hasattr(Lease, "unit_name")
    has_unit_label = hasattr(Lease, "unit_label")

    if unit_id is not None and has_unit_id:
        q = q.where(getattr(Lease, "unit_id") == int(unit_id))
    elif unit_name and has_unit_name:
        q = q.where(getattr(Lease, "unit_name") == str(unit_name))
    elif unit_name and has_unit_label:
        q = q.where(getattr(Lease, "unit_label") == str(unit_name))
    else:
        # No unit scoping available -> property-wide overlap protection.
        pass

    if ignore_lease_id is not None:
        q = q.where(Lease.id != int(ignore_lease_id))

    rows = db.scalars(q.order_by(Lease.id.desc())).all()

    # Determine the model's date fields
    # Common patterns: start_date/end_date, lease_start/lease_end, start_at/end_at
    for r in rows:
        r_start = _as_date(getattr(r, "start_date", None) or getattr(r, "lease_start", None) or getattr(r, "start_at", None))
        r_end = _as_date(getattr(r, "end_date", None) or getattr(r, "lease_end", None) or getattr(r, "end_at", None))

        if r_start is None:
            # corrupt row; skip rather than crash
            continue

        if _overlaps(s, e, r_start, r_end):
            # Conflict found
            raise ValueError(
                f"lease dates overlap with existing lease id={int(getattr(r, 'id'))} "
                f"({r_start.isoformat()} → {(r_end.isoformat() if r_end else 'open-ended')})"
            )
        