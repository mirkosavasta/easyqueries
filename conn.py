import paramiko
import sshtunnel
from sqlalchemy import create_engine
import pandas as pd

from db_credentials import config
"""db_credentials is a .py file that contains a dict of dicts called config. Every sub-dictionary contains
ip addresses, username and psws that are used by Connection to query a given DataBase."""


SSH_KEY_PATH = '/Users/username/.ssh/id_rsa'


class Connection:
"""With this class you can query a MySQL DB and obtain a pandas dataframes as a result."""

    def __init__(self, env, db=None, filename=SSH_KEY_PATH):
        """env: environment. Can be one of the following:
        - Environment 1: ENV1
        - Environment 2: ENV2
        - Environment 3: ENV3
        """
        self.env = env
        ## Access user and psw for the selected environment
        self.env_user = config[env]["user"]
        self.env_psw = config[env]["psw"]
        db = db or config[env].get('db')
        if not db:
        	raise ValueError("No DB Specified.")
        self.db = db
        self.local_port = str(config[self.env]["local_port"])
        self.connection = None
        ## Create rsa_key object
        self.key = paramiko.RSAKey.from_private_key_file(filename=filename)
        ## Create sshtunnel object
        fwd = config["MySQLfwd"]
        self.ssh_connection = sshtunnel.SSHTunnelForwarder(
            (fwd["ip"], fwd["remote_port"]),
            ssh_pkey=self.key,
            ssh_username=fwd["user"],
            remote_bind_address=(config[env]["ip"], config[env]["remote_port"]),
            local_bind_address=("127.0.0.1", config[env]["local_port"])
        )

    def is_active(self):
        ## Checks whether the tunnel is active or not
        return self.ssh_connection.is_active

    def connect(self):
        ## Create ssh tunnel
        self.ssh_connection.start()
        ## SQLALchemy connection string
        connection_string = 'mysql+pymysql://{user}:{pwd}@127.0.0.1:{port}/{db}'.format(
            user=self.env_user,
            pwd=self.env_psw,
            port=self.local_port,
            db=self.db
        )

        ## SQLAlchemy engine
        engine = create_engine(connection_string)
        self.connection = engine.connect()

    def __enter__(self):
        """Enter method for context management"""
        self.connect()
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        self.disconnect()

    def disconnect(self):
        self.connection.close()
        self.ssh_connection.stop()
        self.connection = None

    def get_dataframe(self, sql_stmt):
        if not self.connection:
            raise RuntimeError("No Connection to Database.")
        result_proxy = self.connection.execute(sql_stmt)
        ## Creates the Pandas DataFrame
        return pd.DataFrame(result_proxy.fetchall(), columns=result_proxy.keys())
