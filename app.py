"""App."""
import data_integration
from DAL.DBConnector import DBConnector


def main():
    """Entry point.

    Station ID : stop_area:OCE:SA:87734004

    TODO test all gares

    """
    db = DBConnector()
    #data_integration.stations()
    #data_integration.towns()
    data_integration.towns('duration')


if __name__ == "__main__":
    # execute only if run as a script
    main()
