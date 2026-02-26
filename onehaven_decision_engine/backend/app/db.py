# backend/app/db.py
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from .config import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True,
)


def get_db():
    """
    IMPORTANT (Postgres):
    If any SQL statement fails, the transaction is aborted and the session
    cannot run further statements until a rollback happens.

    This dependency guarantees rollback on exceptions so errors don't cascade
    into "InFailedSqlTransaction" on later queries in the same request.
    """
    db = SessionLocal()
    try:
        yield db
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()
        