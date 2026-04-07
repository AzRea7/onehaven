from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.models import Inspection, Organization, Property, PropertyPhoto, PropertyState


@pytest.fixture
def step19_seed(db_session):
    org = Organization(slug="step19-org", name="Step19 Org")
    db_session.add(org)
    db_session.commit()
    db_session.refresh(org)

    property_a = Property(
        org_id=org.id,
        address="101 Scheduler St",
        city="Warren",
        state="MI",
        zip="48091",
        county="Macomb",
        bedrooms=3,
        bathrooms=1.0,
        square_feet=1200,
        year_built=1965,
        has_garage=False,
        property_type="single_family",
    )
    property_b = Property(
        org_id=org.id,
        address="202 Reminder Ave",
        city="Detroit",
        state="MI",
        zip="48201",
        county="Wayne",
        bedrooms=2,
        bathrooms=1.0,
        square_feet=950,
        year_built=1950,
        has_garage=False,
        property_type="single_family",
    )
    db_session.add_all([property_a, property_b])
    db_session.commit()
    db_session.refresh(property_a)
    db_session.refresh(property_b)

    db_session.add_all(
        [
            PropertyState(
                org_id=org.id,
                property_id=property_a.id,
                current_stage="compliance",
                constraints_json="{}",
                outstanding_tasks_json="{}",
                updated_at=datetime.utcnow(),
            ),
            PropertyState(
                org_id=org.id,
                property_id=property_b.id,
                current_stage="compliance",
                constraints_json="{}",
                outstanding_tasks_json="{}",
                updated_at=datetime.utcnow(),
            ),
        ]
    )
    db_session.commit()

    inspection_a = Inspection(
        org_id=org.id,
        property_id=property_a.id,
        inspection_date=datetime.utcnow() + timedelta(days=2),
        passed=False,
        reinspect_required=True,
        notes="Initial compliance inspection",
        template_key="hud_52580a",
        template_version="hud_52580a_2019",
        jurisdiction="Warren, MI",
    )
    inspection_b = Inspection(
        org_id=org.id,
        property_id=property_b.id,
        inspection_date=datetime.utcnow() + timedelta(hours=3),
        passed=False,
        reinspect_required=True,
        notes="Reminder target inspection",
        template_key="hud_52580a",
        template_version="hud_52580a_2019",
        jurisdiction="Detroit, MI",
    )
    db_session.add_all([inspection_a, inspection_b])
    db_session.commit()
    db_session.refresh(inspection_a)
    db_session.refresh(inspection_b)

    return {
        "org": org,
        "property_a": property_a,
        "property_b": property_b,
        "inspection_a": inspection_a,
        "inspection_b": inspection_b,
    }


@pytest.fixture
def photo_seed(db_session, step19_seed):
    org = step19_seed["org"]
    prop = step19_seed["property_a"]
    inspection = step19_seed["inspection_a"]

    rows = [
        PropertyPhoto(
            org_id=org.id,
            property_id=prop.id,
            source="upload",
            kind="interior",
            label="Kitchen outlet wall",
            url="https://example.test/interior-1.jpg",
            storage_key=None,
            content_type="image/jpeg",
            sort_order=0,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        ),
        PropertyPhoto(
            org_id=org.id,
            property_id=prop.id,
            source="upload",
            kind="exterior",
            label="Front porch rail",
            url="https://example.test/exterior-1.jpg",
            storage_key=None,
            content_type="image/jpeg",
            sort_order=1,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        ),
    ]
    db_session.add_all(rows)
    db_session.commit()

    return {
        "org": org,
        "property": prop,
        "inspection": inspection,
        "photos": rows,
    }
