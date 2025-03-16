import os
from typing import List, Optional
from src.utils.logging import LOGGER
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    String,
    Float,
    create_engine,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)
from datetime import datetime
import json
from datetime import datetime, date
from sqlalchemy.orm import DeclarativeBase

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "..", "..", "..", "db", "database.sqlite3")
db_path = os.path.normpath(db_path)

engine = create_engine(f"sqlite:///{db_path}", echo=True)
Session = sessionmaker(bind=engine)

class Base(DeclarativeBase):
    pass

class Sites(Base):
    __tablename__ = "sites"

    __table_args__ = (
        CheckConstraint(
            "store_format IN ('MAIN STORE', 'MARINE MART')",
            name="chk_store_type",
        ),
    )

    site_id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    site_name: Mapped[Optional[str]] = mapped_column(String(50))
    command_name: Mapped[Optional[str]] = mapped_column(String(30))
    store_format: Mapped[str] = mapped_column(String(20), nullable=False)

    def __repr__(self) -> str:
        return (
            f"Sites(site_id={self.site_id!r}, "
            f"site_name={self.site_name!r}, "
            f"command_name={self.command_name!r}, "
            f"store_format={self.store_format!r})"
        )


class Metrics(Base):
    __tablename__ = "metrics"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    metric_name: Mapped[str] = mapped_column(String(50), nullable=False)
    metric_desc: Mapped[Optional[str]] = mapped_column(String(200))
    is_retail: Mapped[bool] = mapped_column(Boolean, default=False)
    is_marketing: Mapped[bool] = mapped_column(Boolean, default=False)
    is_survey: Mapped[bool] = mapped_column(Boolean, default=False)
    is_daily: Mapped[bool] = mapped_column(Boolean, default=False)
    is_monthly: Mapped[bool] = mapped_column(Boolean, default=False)
    is_quarterly: Mapped[bool] = mapped_column(Boolean, default=False)
    is_yearly: Mapped[bool] = mapped_column(Boolean, default=False)

    facts: Mapped[List["Facts"]] = relationship(
        back_populates="metric", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return (
            f"Metrics(id={self.id!r}, "
            f"metric_name={self.metric_name!r}, "
            f"metric_desc={self.metric_desc!r})"
        )


class PeriodDim(Base):
    __tablename__ = "period_dim"

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    period_name: Mapped[str] = mapped_column(String(15), nullable=False)

    def __repr__(self) -> str:
        return f"PeriodDim(id={self.id!r}, " f"period_name={self.period_name!r})"


class Facts(Base):
    __tablename__ = "facts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    metric_id: Mapped[Optional[int]] = mapped_column(ForeignKey("metrics.id"))
    group_name: Mapped[Optional[str]] = mapped_column(String(50))
    value: Mapped[Optional[float]] = mapped_column(Float)
    date: Mapped[Optional[datetime]] = mapped_column(Date)
    period_level: Mapped[Optional[int]] = mapped_column()
    record_inserted_date: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow
    )

    metric: Mapped[Optional[Metrics]] = relationship("Metrics", back_populates="facts")

    def __repr__(self) -> str:
        return (
            f"Facts(id={self.id!r}, metric_id={self.metric_id!r}, "
            f"group_name={self.group_name!r}, value={self.value!r}, "
            f"date={self.date!r}, period_level={self.period_level!r}, "
            f"record_inserted_date={self.record_inserted_date!r})"
        )




class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # Convert date/datetime
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        # Convert SQLAlchemy models
        if isinstance(obj, DeclarativeBase):
            # Optionally convert to dict, or just pick fields
            # if you have a to_dict() method you could do:
            # return obj.to_dict()
            # Or do something like:
            return {col.name: getattr(obj, col.name)
                    for col in obj.__table__.columns}
        return super().default(obj)

if __name__ == "__main__":

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    LOGGER.info(f"Base dir - {BASE_DIR}")
    db_path = os.path.join(BASE_DIR, "..", "..", "..", "db", "database.sqlite3")
    db_path = os.path.normpath(db_path)
    LOGGER.info(f"DB path - {db_path}")
    LOGGER.info(db_path)

    engine = create_engine(f"sqlite:///{db_path}", echo=True)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    new_metric = Metrics(
        metric_name="Daily Sales", metric_desc="Track daily sales", is_daily=True
    )
    session.add(new_metric)
    session.commit()
    LOGGER.info(f"Inserted metric with ID {new_metric.id}")

    metric = session.query(Metrics).filter_by(metric_name="Daily Sales").first()
    LOGGER.info(f"Fetched metric: {metric}")

    session.close()
