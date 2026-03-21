from __future__ import annotations

from datetime import datetime

import pytest

from app.models import Inspection, Organization, Property, PropertyState


@pytest.fixture
def real_inspection_seed(db_session):
    org = Organization(slug="step18-org", name="Step18 Org")
    db_session.add(org)
    db_session.commit()
    db_session.refresh(org)

    prop = Property(
        org_id=org.id,
        address="100 Real Form Dr",
        city="Warren",
        state="MI",
        zip="48091",
        county="Macomb",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1250,
        year_built=1960,
        has_garage=True,
        property_type="single_family",
    )
    db_session.add(prop)
    db_session.commit()
    db_session.refresh(prop)

    db_session.add(
        PropertyState(
            org_id=org.id,
            property_id=prop.id,
            current_stage="compliance",
            constraints_json="{}",
            outstanding_tasks_json="{}",
            updated_at=datetime.utcnow(),
        )
    )
    db_session.commit()

    return {
        "org": org,
        "property": prop,
    }


@pytest.fixture
def real_inspection(db_session, real_inspection_seed):
    org = real_inspection_seed["org"]
    prop = real_inspection_seed["property"]

    insp = Inspection(
        org_id=org.id,
        property_id=prop.id,
        inspection_date=datetime.utcnow(),
        passed=False,
        reinspect_required=True,
        notes="step18 inspection",
    )
    db_session.add(insp)
    db_session.commit()
    db_session.refresh(insp)
    return insp