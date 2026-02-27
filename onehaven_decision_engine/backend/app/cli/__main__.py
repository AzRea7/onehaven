# backend/app/cli/__main__.py
from __future__ import annotations

import argparse

from app.cli.seed_demo import seed_demo


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--org-slug", default="demo")
    p.add_argument("--org-name", default="demo")
    p.add_argument("--user-email", default="austin@demo.local")
    p.add_argument("--user-name", default="Austin")
    p.add_argument("--plan-code", default="free", choices=["free", "pro"])
    p.add_argument("--no-sample-property", action="store_true")
    args = p.parse_args()

    out = seed_demo(
        org_slug=args.org_slug,
        org_name=args.org_name,
        user_email=args.user_email,
        user_name=args.user_name,
        plan_code=args.plan_code,
        create_sample_property=(not args.no_sample_property),
    )
    print(
        {
            "ok": True,
            "org_slug": out.org_slug,
            "user_email": out.user_email,
            "plan_code": out.plan_code,
            "sample_property_id": out.property_id,
        }
    )


if __name__ == "__main__":
    main()
    