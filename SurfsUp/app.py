# Import the dependencies.
from flask import Flask, jsonify
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session, sessionmaker, scoped_session  #added to be able to run different threads:  https://docs.sqlalchemy.org/en/20/orm/contextual.html#sqlalchemy.orm.scoped_session

#################################################
# Database Setup
#################################################

# Create an engine to connect to the SQLite database
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model

# reflect the tables
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)


#################################################
# Flask Routes
#################################################
# Homepage route to list available routes
@app.route("/")
def homepage():
    return (
        "Welcome to the Hawaii Climate API!<br/>"
        "Available Routes:<br/>"
        "/api/v1.0/precipitation<br/>"
        "/api/v1.0/stations<br/>"
        "/api/v1.0/tobs<br/>"
        "/api/v1.0/&lt;start&gt;<br/>"
        "/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
    )

# Precipitation route to return last 12 months of precipitation data
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Calculate the date one year from the last date in data set.
    most_recent_date_row = session.query(func.max(Measurement.date)).one()
    most_recent_date_str = most_recent_date_row[0]  # Extract the date value from the Row as a string

    # Convert the date string to a datetime object
    most_recent_date = dt.datetime.strptime(most_recent_date_str, '%Y-%m-%d')

    one_year_ago = most_recent_date - dt.timedelta(days=365)
    
    # Query last 12 months of precipitation data
    last_12_months_precipitation = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago).all()
    
    # Convert the query results to a dictionary
    precipitation_dict = {date: prcp for date, prcp in last_12_months_precipitation}
    
    return jsonify(precipitation_dict)

# Stations route to return list of stations
@app.route("/api/v1.0/stations")
def stations():
    # Create a new session for this route
    session = Session(engine)

    try:
        # Query list of stations
        stations = session.query(Station.station).all()

        # Convert the query results to a list
        stations_list = [station[0] for station in stations]

        return jsonify(stations_list)
    finally:
        session.close()  # Close the session after the query


# Temperature observations route for the most active station
@app.route("/api/v1.0/tobs")
def tobs():
    # Create a new session for this route
    session = Session(engine)

    try:
        # Calculate the date one year from the last date in data set.
        most_recent_date_row = session.query(func.max(Measurement.date)).one()
        most_recent_date_str = most_recent_date_row[0]  # Extract the date value from the Row as a string

        # Convert the date string to a datetime object
        most_recent_date = dt.datetime.strptime(most_recent_date_str, '%Y-%m-%d')

        one_year_ago = most_recent_date - dt.timedelta(days=365)

        # Determine the most active station
        most_active_station = session.query(Measurement.station).\
            group_by(Measurement.station).\
            order_by(func.count(Measurement.station).desc()).\
            first()[0]

        # Query last 12 months of temperature observations for most active station
        last_12_months_tobs = session.query(Measurement.date, Measurement.tobs).\
            filter(Measurement.station == most_active_station, Measurement.date >= one_year_ago).all()

        # Convert the query results to a list of dictionaries
        tobs_list = [{"date": date, "tobs": tobs} for date, tobs in last_12_months_tobs]

        return jsonify(tobs_list)
    finally:
        session.close()  # Close the session after the query

# Summary statistics route for a specific start date
@app.route("/api/v1.0/<string:start>")
def start_date_summary(start):
    try:
        # Convert the input start date to a datetime object using the 'YYYYMMDD' format
        start_date = dt.datetime.strptime(start, '%Y%m%d')

        # Query summary statistics from start date onwards
        temperature_stats = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date).all()

        # Check if there are any results returned
        if temperature_stats:
            min_temp, avg_temp, max_temp = temperature_stats[0]

            summary_dict = {
                "start_date": start_date.strftime('%Y-%m-%d'),
                "min_temperature": min_temp,
                "avg_temperature": avg_temp,
                "max_temperature": max_temp
            }

            return jsonify(summary_dict)
        else:
            return jsonify({"message": "No data available for the specified start date."})
    except ValueError:
        return jsonify({"error": "Invalid date format. Please provide a date in the 'YYYYMMDD' format."})
    except Exception as e:
        return jsonify({"error": str(e)})

# Summary statistics route for a date range
@app.route("/api/v1.0/<string:start>/<string:end>")
def date_range_summary(start, end):
    try:
        # Convert the input start and end dates to datetime objects using the 'YYYY-MM-DD' format
        start_date = dt.datetime.strptime(start, '%Y-%m-%d')
        end_date = dt.datetime.strptime(end, '%Y-%m-%d')

        # Query summary statistics within the specified date range
        temperature_stats = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date, Measurement.date <= end_date).all()

        # Check if there are any results returned
        if temperature_stats:
            min_temp, avg_temp, max_temp = temperature_stats[0]

            summary_dict = {
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "min_temperature": min_temp,
                "avg_temperature": avg_temp,
                "max_temperature": max_temp
            }

            return jsonify(summary_dict)
        else:
            return jsonify({"message": "No data available for the specified date range."})
    except ValueError:
        return jsonify({"error": "Invalid date format. Please provide dates in the 'YYYY-MM-DD' format."})
    except Exception as e:
        return jsonify({"error": str(e)})

# Run the app if executed directly
if __name__ == "__main__":
    app.run(debug=True)