from datetime import datetime

from sqlalchemy import Column
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ValidatorLogger(Base):
    __tablename__ = 'validator_logs'
    log_id = Column(postgresql.VARCHAR(100), primary_key=True)
    resource_id = Column(postgresql.VARCHAR(100), nullable=False)
    package_id = Column(postgresql.VARCHAR(100), nullable=False)
    database = Column(postgresql.VARCHAR(250), nullable=False)
    table = Column(postgresql.VARCHAR(250), nullable=False)
    description = Column(postgresql.TEXT)
    created_time = Column(postgresql.TIMESTAMP, default=datetime.utcnow)
