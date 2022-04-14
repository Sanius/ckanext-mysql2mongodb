from sqlalchemy import Table, Column
from sqlalchemy.dialects import postgresql

from ckanext.mysql2mongodb.dataconv.database.schema import metadata

ValidatorLogger = Table('validator_log', metadata,
                        Column('log_id', postgresql.VARCHAR(100), primary_key=True),
                        Column('resource_id', postgresql.VARCHAR(100)),
                        Column('package_id', postgresql.VARCHAR(100)),
                        Column('database', postgresql.VARCHAR(250)),
                        Column('table', postgresql.VARCHAR(250)),
                        Column('description', postgresql.TEXT),
                        )
