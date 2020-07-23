"""Setup MariaDB."""
# Module Imports
import sys

import mariadb

import config

PATH_TO_FILE = config.DAL_PATH+'create.sql'


class DBConnector():
    """Connect to MariaDB Platform."""

    def __init__(self):
        """Construct a DBConnector object."""
        try:
            conn = mariadb.connect(
                user=config.USER,
                password=config.PWD,
                host=config.HOST,
                port=config.PORT,
                database=config.DB,
                init_command='SET NAMES UTF8'
            )
        except mariadb.Error as e:
            print(f'Error connecting to MariaDB Platform: {e}')
            sys.exit(1)

        cur = conn.cursor()
        self.__executeScriptsFromFile(PATH_TO_FILE, cur)

    def __executeScriptsFromFile(self, filename, cur):
        """Open and read the file as a single buffer.

        Args:
            cur (cursor): cursor

        """
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()

        # all SQL commands (split on ';')
        sqlCommands = sqlFile.split(';')

        # Execute every command from the input file
        for command in sqlCommands:
            # This will skip and report errors
            # For example, if the tables do not yet exist, this will skip over
            # the DROP TABLE commands
            if command:
                try:
                    cur.execute(command)
                except mariadb.Error as err:
                    print(f'Error executing SQL command: {err}')
