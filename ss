from google.cloud.sql.connector import Connector, IPTypes
import sqlalchemy
import os
from functools import wraps
from asyncio.proactor_events import _ProactorBasePipeTransport

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/002V42744/Downloads/model-hexagon-316809-87ffd89215dd.json"

# Python Connector database creator function
def getconn():
    with Connector() as connector:
        conn = connector.connect(
            "model-hexagon-316809:us-central1:hive-source", # Cloud SQL Instance Connection Name
            "pymysql",
            user="root",
            password="root",
            db="test"
            # ip_type=IPTypes.PUBLIC # IPTypes.PRIVATE for private IP
        )
    return conn

# create SQLAlchemy connection pool
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

# interact with Cloud SQL database using connection pool
with pool.connect() as db_conn:
    # query database
    result = db_conn.execute("SELECT * from corona_pandemic_table limit 5").fetchall()

    # Do something with the results
    for row in result:
        print(row)

def silence_event_loop_closed(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as e:
            if str(e) != 'Event loop is closed':
                raise
    return wrapper
_ProactorBasePipeTransport.__del__ = silence_event_loop_closed(_ProactorBasePipeTransport.__del__)