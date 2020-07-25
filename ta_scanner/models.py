from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Numeric,
    String,
    DateTime,
    Index,
    Boolean,
)


Base = declarative_base()


NUMERIC_OPTIONS = dict(precision=8, scale=2, decimal_return_scale=None, asdecimal=True)


def gen_engine():
    connection_string: str = "postgresql://ta_scanner:ta_scanner@localhost:65432/ta_scanner"
    engine = create_engine(connection_string, convert_unicode=True)
    return engine


def init_db():
    engine = gen_engine()
    Base.metadata.create_all(bind=engine)


class Quote(Base):
    __tablename__ = "quote"

    id = Column(Integer, primary_key=True)
    ts = Column(DateTime(timezone=True), index=True)
    symbol = Column(String(10))
    open = Column(Numeric(**NUMERIC_OPTIONS))
    close = Column(Numeric(**NUMERIC_OPTIONS))
    high = Column(Numeric(**NUMERIC_OPTIONS))
    low = Column(Numeric(**NUMERIC_OPTIONS))
    average = Column(Numeric(**NUMERIC_OPTIONS))
    volume = Column(Integer)
    bar_count = Column(Integer)
    rth = Column(Boolean)

    __table_args__ = (Index("ix_quote_symbol_ts", symbol, ts, unique=True),)
