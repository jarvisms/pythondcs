from datetime import date, timedelta
import pandas as pd
import pythondcs

with pythondcs.DcsWebApi("https://energy.warwick.ac.uk/dcswebapi") as dcs:
    df = pd.DataFrame( dcs.largereadings("R839", startTime=date(2022,1,1), endTime=date.today(), maxwindow=timedelta(days=14), iterator=True)['readings'] )
