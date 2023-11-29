import time

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import logging



# set the logging output file
logging.basicConfig(filename='database.log', level=logging.INFO)
logger = logging.getLogger(__name__)



base_class = declarative_base()

class Event(base_class):
    """
    Event class to store the events.
    """
    __tablename__ = 'events'
    id = sa.Column(sa.Integer, primary_key=True)
    time = sa.Column(sa.DateTime, nullable=False)
    type = sa.Column(sa.String(50), nullable=False)

# initialize the database
def init_db():
    """
    Initializes the database.
    """
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres')
    Session = sessionmaker(bind=engine)
    session = Session()
    print("Database connection initialized.")

    # Create the tables
    base_class.metadata.create_all(engine)
    print("Tables Created.")

    # Insert demo data into the database
    events = [
        ("2023-05-10T18:30:30", "pedestrian"),
        ("2023-05-10T18:31:00", "pedestrian"),
        ("2023-05-10T18:31:00", "car"),
        ("2023-05-10T18:31:30", "pedestrian"),
        ("2023-05-10T18:35:00", "pedestrian"),
        ("2023-05-10T18:35:30", "pedestrian"),
        ("2023-05-10T18:36:00", "pedestrian"),
        ("2023-05-10T18:37:00", "pedestrian"),
        ("2023-05-10T18:37:30", "pedestrian"),
    ]
    session.add_all([Event(time=timestamp, type=event_type) for timestamp, event_type in events])
    session.commit()
    print("Demo data inserted.")
    session.close()
    print("Database connection closed. \n Database initialized.")

def get_database_engine():
    """
    Creates a connection to the database.
    """
    # create the database engine
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=True)
    # return the engine
    conn = engine
    logger.info("Database connection initialized.")
    return conn

def database_connection():
    """
    Creates a connection to the database.
    """
    # create the database engine
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/postgres', echo=True)
    # create the database connection
    conn = engine.connect()
    # create the table
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS events (\
                id SERIAL PRIMARY KEY,\
                time TIMESTAMP NOT NULL,\
                type VARCHAR(50) NOT NULL\
            );"
        )
    )
    # return the connection
    return conn

def ingest_event(db_engine, timestamp: str, event_type: str):
    """
    Ingests data into the database.
    """
    # create the event
    new_event = Event(time=timestamp, type=event_type) 
    # create a session
    Session = sessionmaker(bind=db_engine)
    session = Session()
    # add the new event
    session.add(new_event)
    # commit the session
    session.commit()
    # test the new event is in the database
    assert test_ingest_data(db_engine, timestamp, event_type)
    # log the event
    logger.info(f"Event ingested: {new_event}")
    # check if there are 5 consecutive person detections
    if event_type == "pedestrian":
        print("\nChecking for consecutive pedestrian detections...\n")
        check_consecutive_person_detections(session)
    # close the session
    session.close()

def test_ingest_data(session, timestamp: str, event_type: str):
    """
    Tests the ingest_data function.
    """
    Session = sessionmaker(bind=session)
    session = Session()
    result = session.query(Event).filter(Event.time == timestamp, Event.type == event_type).first()
    session.close()
    if result:
        return True
    else:
        return False
  
def aggregate_events(conn) -> dict[str, list[tuple[str, str]]]:
    """
    Aggregates events from the database based on event type and time.
    """
    def format_datetime(time):
        """
        Formats the time.
        """
        return time.strftime("%Y-%m-%dT%H:%M:%S")
    def format_result(results):
        """
        Formats the result.
        """
        result_dict = {}
        for start_time, end_time, category in results:
            start_time_str = format_datetime(start_time)
            end_time_str = format_datetime(end_time)

            if category.lower() not in result_dict:
                result_dict[category.lower()] = []
            result_dict[category.lower()].append((start_time_str, end_time_str))
        return result_dict
    # create a session
    Session = sessionmaker(bind=conn)
    session = Session()
    logger.info("Database connection initialized.")
    logger.info("Retreiving results...")
    # create the SQL statement
    sqL_statement = sa.text("""WITH grouped_events AS (
  SELECT
    time,
    type,
    CASE
      WHEN type IN ('pedestrian', 'bicycle') THEN 'People'
      WHEN type IN ('car', 'truck', 'van') THEN 'Vehicles'
    END AS type_group,
    LAG(time) OVER (ORDER BY time) AS prev_time
  FROM events
) SELECT
  MIN(time) AS start_time,
  MAX(time) AS end_time,
  type_group
FROM grouped_events
GROUP BY
  type_group,
  CASE
    WHEN EXTRACT(EPOCH FROM (time - prev_time)) <= 60 THEN 0
    ELSE 1
  END
ORDER BY start_time;
"""
    )
    results = session.execute(sqL_statement).fetchall()
    session.close()
    # check if there are any results
    if not results:
        logger.info("No results found.")
        return {}
    # format the results
    results = format_result(results)
    logger.info("Results aggregated.")
    return results

def check_consecutive_person_detections(session):
    """
    Checks if there are 5 consecutive person detections.
    """
    # set the consecutive count and threshold
    consecutive_count = 0
    threshold = 5

    # Query the database for the latest events
    latest_events = session.query(Event).order_by(Event.time.desc()).limit(threshold)
    logger.info("Checking latest events.")
    print("Checking latest events.")

    for event in latest_events:
        if event.type.lower() == 'person':
            consecutive_count += 1
        else:
            break  # Reset count if a non-person category is encountered

    if consecutive_count >= threshold:
        logger.info(f"Alert: Person detected in {consecutive_count} consecutive events!")
        print(f"Alert: Person detected in {consecutive_count} consecutive events!")

def main():
    conn = get_database_engine()

    # Simulate real-time events every 30 seconds
    events = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]
    for time, type in events:
        ingest_event(conn, time, type)

    agg_events = aggregate_events(conn)
    print(agg_events)
            
        # Simulate real-time events every 30 seconds
    # events = [
    #     ("2023-08-10T18:30:30", "pedestrian"),
    #     ("2023-08-10T18:31:00", "pedestrian"),
    #     ("2023-08-10T18:31:00", "car"),
    #     ("2023-08-10T18:31:30", "pedestrian"),
    #     ("2023-08-10T18:35:00", "pedestrian"),
    #     ("2023-08-10T18:35:30", "pedestrian"),
    #     ("2023-08-10T18:36:00", "pedestrian"),
    #     ("2023-08-10T18:37:00", "pedestrian"),
    #     ("2023-08-10T18:37:30", "pedestrian"),
    # ]
    # for timestamp, event_type in events:
    #     ingest_data(conn, timestamp, event_type)
    # 
    # print(aggregate_events(conn))
    # 

if __name__ == "__main__":
    # main()
    # init_db()
    main()
    