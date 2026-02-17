from datetime import date, timedelta
import sqlite3
import pythondcs

sql=sqlite3.connect(':memory:')
_ = sql.executescript("""
CREATE TABLE Readings (
dataID,
timestamp,
value,
status
);
""")

itemsOfInterest = ["R839", "VM88"]

with pythondcs.DcsWebApi("https://energy.warwick.ac.uk/dcswebapi") as dcs:
    for id in itemsOfInterest:
        data = dcs.largereadings(id, startTime=date(2022,1,1), endTime=date.today(), maxwindow=timedelta(days=14), iterator=True)
        converteddata = ( {'dataID':id, **read } for read in data['readings'] )
        response = sql.executemany("INSERT INTO Readings VALUES (:dataID, :timestamp, :value, :status);", converteddata)
        sql.commit()
        print( f"Written {response.rowcount} records for id {id}")
