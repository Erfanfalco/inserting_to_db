import psycopg2
import logging
from configparser import ConfigParser


class DatabaseConnection:
    def __init__(self, config_file_path):
        self.config = ConfigParser()
        self.config.read(config_file_path)
        self.connection = None
        self.autocommit = True

    def connect(self):
        """Establishes a connection to the database."""
        try:
            host = self.config.get('my_db', 'host')
            dbname = self.config.get('my_db', 'dbname')
            user = self.config.get('my_db', 'user')
            password = self.config.get('my_db', 'password')
            self.connection = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
            logging.info("Database connection established.")
        except Exception as e:
            logging.error(f"Failed to establish database connection: {e}")
            raise

    def get_connection(self):
        return self.connection

    def close(self):
        """Closes the database connection."""
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed.")

    def get_cursor(self):
        """Returns a new cursor object."""
        if self.connection:
            return self.connection.cursor()
        else:
            logging.error("No active database connection.")
            return None
