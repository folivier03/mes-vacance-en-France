"""Setup MariaDB."""
# Module Imports
import sys

import mariadb

import config

PATH_TO_FILE = config.DAL_PATH+'create.sql'


class DBConnector():
    """Connect to MariaDB Platform."""

    __instance = None

    @staticmethod
    def getInstance():
        """Get instance via static access method."""
        if DBConnector.__instance is None:
            DBConnector()
        return DBConnector.__instance

    def __init__(self):
        """Construct a DBConnector object.

        Virtually private constructor.

        """
        if DBConnector.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            DBConnector.__instance = self

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

        self.cur = conn.cursor()
        self.__executeScriptsFromFile(PATH_TO_FILE)

    def __executeScriptsFromFile(self, filename):
        """Open and read the file as a single buffer.

        Args:
            cur (cursor): cursor

        """
        with open(filename, 'r') as fd:
            sqlFile = fd.read()

        # all SQL commands (split on ';')
        sqlCommands = sqlFile.split(';')

        # Execute every command from the input file
        for command in sqlCommands:
            # This will skip and report errors
            # For example, if the tables do not yet exist, this will skip over
            # the DROP TABLE commands
            if command:
                try:
                    self.cur.execute(command)
                except mariadb.Error as err:
                    print(f'Error executing SQL command: {err}')

    def build_select_query(self, field, table, col, val):
        """Select query builder.

        Args:
            field (str): field to select
            table (str): table name
            col (str): condition column name
            val (str): condition column value

        """
        self.cur.execute(f'SELECT {field} FROM {table} WHERE {col} = {val};')
        row = self.cur.fetchone()

        if row is not None:
            return row[0]
        else:
            return None

    def build_insert_query(self, table, cols, vals):
        """Insert query builder.

        Args:
            table (str): table name
            cols (str): columns names
            vals (str): columns values

        """
        query = f'INSERT INTO {table} ({cols}) VALUES ({vals});'

        self.cur.execute(query)
