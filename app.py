"""App."""
import data_integration
from DAL.DBConnector import DBConnector


def main():
    """Entry point.

    data_integration.stations(): lauch only once to insert stations in db
    data_integration.towns(): launch it to get the w/ minimum CO2 emit
    data_integration.towns('duration'): launch it to get the w/ minimum
    duration time
    """
    db = DBConnector()
    #data_integration.stations()
    #data_integration.towns()
    data_integration.towns('duration')


if __name__ == "__main__":
    # execute only if run as a script
    main()
