import numpy as np
import pandas as pd
import datetime as dt 

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)
inspector = inspect(engine)

meas = engine.execute('SELECT station, date, prcp, tobs FROM measurement ORDER BY date DESC').fetchall()
stat = engine.execute('SELECT station, name, latitude, longitude, elevation FROM station').fetchall()

# Get the column names, for loop is to skip the 'ID' which is not imported
mcolumns = inspector.get_columns('measurement')
mcol = []
counter=0
for c in mcolumns:
    if counter > 0:
        mcol.append(c['name'])
    counter = counter + 1

scolumns = inspector.get_columns('station')
scol = []
counter=0
for c in scolumns:
    if counter > 0:
        scol.append(c['name'])
    counter = counter + 1

# Import into dataframes and add column labels
measdf = pd.DataFrame(meas)
measdf.columns = mcol
measdf = measdf.drop(columns='tobs')
statdf = pd.DataFrame(stat)
statdf.columns = scol

# Merge the Dataframes, because that just makes life easier for whatever we want to do and why complicate things
total = pd.merge(measdf, statdf, on='station')

# Get the last date
ldate = total.iloc[0]['date']
fdate = dt.datetime(2017, 8, 23) - dt.timedelta(days=365)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precip<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/activity<br/>"
        f"/api/v1.0/tempavgactive<br/>"
        f"/api/v1.0/actstats<br/>"
    )


@app.route("/api/v1.0/precip")
def precip ():
    """Describe the last 365 days of precipitation"""
    # Query all passengers
    last365 = measdf[pd.to_datetime(measdf.date, format='%Y-%m-%d') >= fdate]

    # Convert list of tuples into normal list
    desc = last365.describe()
    desc = desc.to_dict()

    return jsonify(desc)


@app.route("/api/v1.0/stations")
def stations():
    """Tell us how many stations there are"""
    # Query all passengers
    scount = total.station.nunique()
    scount = scount.to_dict()
    return jsonify(scount)


@app.route("/api/v1.0/activity")
def activity():
    """Tell us the most active stations"""
    # Query all passengers
    activity = measdf.groupby('station').count().drop(columns='tobs').sort_values(by=['date'], ascending=False).drop(columns=['prcp']).rename(columns={'date': 'Count'})
    activity = activity.to_dict()
    return jsonify(activity)

@app.route("/api/v1.0/tempavgactive")
def tempavgact():
    """Tell us how many stations there are"""
    # Query all passengers
    measdf2 = pd.DataFrame(meas)
    measdf2.columns = mcol
    active = measdf2.station.mode()[0]
    mactive = measdf2[measdf2.station == active]
    mamt = mactive.tobs.max()
    malt = mactive.tobs.min()
    maat = mactive.tobs.mean()
    #print(f'The lowest temperature recorded was {malt}.')
    #print(f'The highest tempertaure recorded was {mamt}.') 
    #print(f'The average temperature recorded was {round(maat,2)}.')

    return jsonify(malt, mamt, maat)

@app.route("/api/v1.0/actstats")
def actstats():
    """Tell us the rain at the most active stations"""
    # Query all passengers
    actstat = measdf.groupby('station').count().drop(columns='tobs').sort_values(by=['date'], ascending=False).drop(columns=['prcp']).rename(columns={'date': 'Count'})
    actstat = actstat.to_dict()

    return jsonify(actstat)

if __name__ == '__main__':
    app.run(debug=True)
