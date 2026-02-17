from sys import stdout
import logging
logging.basicConfig(stream=stdout, format='%(asctime)s %(message)s',level=logging.INFO)
logging.info("Starting...")

from datetime import datetime, date, timedelta, timezone
import getpass, sqlite3, pythondcs

sql=sqlite3.connect(r'/media/disk/DCS.db')

with sql:
    # My standard SQLite Prep
    # The table just has dataID, timestamp and value. Ignoring status for simplicity. Compound Primary Key over ID and timestamp.
    _ = sql.executescript("""
    PRAGMA journal_mode = wal;
    PRAGMA synchronous = normal;
    PRAGMA auto_vacuum = incremental;
    PRAGMA threads = 10;
    PRAGMA mmap_size = 9223372036854775807;
    PRAGMA temp_store = memory;
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS Readings (
    dataID TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    value REAL NOT NULL,
    PRIMARY KEY (dataID, timestamp)
    ) WITHOUT ROWID;
    """)

with open("ID List.txt", "r") as idfile:
    # Read the IDs, one per line, into a list
    itemsOfInterest = idfile.read().split()

def perioddata(id, totaldata):
    """Generator function works out the differences between the current and the next meter reading - timestamps represent the START of the period
    Provides the ID in each case ready for the SQL table. SQLite doesn't support date/times so converting to unix timestamps integers
    Note: Doesn't care what the duration is between each timestamp, or if there are any resets, glitches etc.
    Inputs: an ID and an iterator/list of Readings
    Yields: Dictionaries resembling the Readings but with delta values and the ID added
    Returned: None
    """
    totaldata = iter(totaldata)
    before = next(totaldata)
    for after in totaldata:
        data = { 'dataID': id, 'timestamp': int(before['timestamp'].timestamp()), 'value': (after['value'] - before['value']) }
        yield data
        before = after

with pythondcs.DcsWebApi("https://energy.warwick.ac.uk/dcswebapi", getpass.getpass("Username: "),getpass.getpass("Password: ")) as dcs:
    for id in itemsOfInterest:  # Loop over all the IDs
        # Get 5 years back from today in 1 year chunks concatenated and streamed as an iterator.
        data = dcs.largereadings(id, startTime=date.today()-timedelta(days=5*365), endTime=date.today(), maxwindow=timedelta(days=365), iterator=True)
        with sql:
            # UPSERT into the database table in one bulk transaction
            response = sql.executemany(
                "INSERT INTO Readings VALUES (:dataID, :timestamp, :value) ON CONFLICT (dataID, timestamp) DO UPDATE SET value = :value;",
                perioddata(id,data['readings'])
            )
            logging.info( f"Written {response.rowcount} records for id {id}" )

with sql:
    # SQLite Mop up
    _ = sql.executescript(
        """
        PRAGMA optimize;
        PRAGMA wal_checkpoint(TRUNCATE);
        PRAGMA incremental_vacuum;
        """)

logging.info(f"Total changes to database: {sql.total_changes:,} (including intermediate processing)")
sql.close()
dcs.signout()
logging.info("Database Closed, DCS Session signed out")
