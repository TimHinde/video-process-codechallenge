import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

import logging

# set the logging output file
logging.basicConfig(filename='database.log', level=logging.INFO)
# create a logger to log the events
logger = logging.getLogger(__name__)
# Create a handler for the logger
handler = logging.StreamHandler()
# Create a formatter for the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


base_class = declarative_base()

class Event(base_class):
    """
    Event class to store the events.
    """
    __tablename__ = 'events'
    id = sa.Column(sa.Integer, primary_key=True)
    time = sa.Column(sa.DateTime, nullable=False)
    type = sa.Column(sa.String(50), nullable=False)

class EventService:
    def __init__(self, db_connection_string):
        self.db_connection_string = db_connection_string
        self.db_engine = create_engine(self.db_connection_string)
        self.init_db()

    def init_db(self):
        engine = self.db_engine
        base_class.metadata.create_all(engine)
        print("Tables Created.")

    def test_duplicate_row(self, timestamp: str, event_type: str):
        Session = sessionmaker(bind=self.db_engine)
        session = Session()
        result = session.query(Event).filter(Event.time == timestamp, Event.type == event_type).first()
        session.close()
        if result:
            return True
        else:
            return False
    
    def insert_one_event(self, timestamp: str, event_type: str):
        """
        Ingests data into the database.
        """
        # Guard against duplicate rows
        if self.test_duplicate_row(timestamp, event_type):
            print("Event already exists.")
            return None
        # create the event
        new_event = Event(time=timestamp, type=event_type) 
        Session = sessionmaker(bind=self.db_engine)
        session = Session()
        session.add(new_event)
        session.commit()
        session.close()
        session.close()
        # test the new event is in the database
        assert self.test_duplicate_row(timestamp, event_type), "Event not inserted."
        # log the event
        logger.info(f"Event ingested: {new_event}")
        # check if there are 5 consecutive person detections
        if event_type == "pedestrian":
            print("\nChecking for consecutive pedestrian detections...\n")
            self.suspicious_person_check()
            
        return new_event
    
    def suspicious_person_check(self):
        # set the consecutive count and threshold
        consecutive_count = 0
        threshold = 5
        # Initialize the session
        Session = sessionmaker(bind=self.db_engine)
        session = Session()
        # Query the database for the latest events
        latest_events = session.query(Event).order_by(Event.time.desc()).limit(threshold)
        logger.info("Checking latest events.")
        print("Checking latest events.")
        # Check if there are 5 consecutive person detections
        for event in latest_events:
            if event.type.lower() == 'pedestrian':
                consecutive_count += 1
            else:
                break # Reset count if a non-person category is encountered
        # Log the alert
        if consecutive_count >= threshold:
            logger.info(f"Alert: Person detected in {consecutive_count} consecutive events!")
            print(f"Alert: Person detected in {consecutive_count} consecutive events!")
        session.close()
        return
    
    def get_aggregate_events(self):
        """
        Aggregates events from the database based on event type and time.
        """
        # Format datetime
        def format_datetime(time):
            """
            Formats the time.
            """
            return time.strftime("%Y-%m-%dT%H:%M:%S")
        # Format funciton
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
        Session = sessionmaker(bind=self.db_engine)
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
        
def main():
    connection_string = 'postgresql+psycopg2://postgres:postgres@postgres:5432/postgres'
    es =  EventService(connection_string)

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
        es.insert_one_event(time, type)

    agg_events = es.get_aggregate_events()
    print(agg_events)

    es.suspicious_person_check()
    
    # 

if __name__ == "__main__":
    # sleep for 10 seconds to allow the database to spin up
    import time
    time.sleep(10)
    main()
