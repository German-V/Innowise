import psycopg2
import db.config as config

"""
A class for managing connections to a PostgreSQL database using psycopg2.
"""

class Connection:
    def __init__(self,logg):
        self.logg = logg
    
    def create_connection(self):
        params = config.config()
        try:
            conn = psycopg2.connect(**params)
            self.logg.info("Connected to database")
            return conn
        except:
            self.logg.exception("Connection failed")
            
    def close_connection(self,conn):
        try:
            conn.close()
            self.logg.info("Connection closed")
        except:
            self.logg.exception("No opened connections")