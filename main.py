import pandas as pd
import sqlalchemy as sa
import logging
from logging import DEBUG, INFO
import sys


logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

class DataLoader():

    def __init__(self, filepath:str) -> None:
        """
        Loads a CSV file path into a Dataframe
        
        Args:
            filepath (str): file path to the CSV file
        """
        # read the csv file into a dataframe using pandas
        df = pd.read_csv(filepath, header=0)
        # assign this class instance to the dateframe
        self.df = df
        pass

    def head(self) -> None:
        """
        prints the head of the dataframe to console
        """
        return self.df.head()

    def add_index(self, index_name:str, column_names:list) -> None:
        """
        Create a dataframe index column from concatenating a series of column values. Column values are concatenated by a dash "-".

        For example if you have three columns such as: artist="Metallica", song="Ride the Lighting"
        the index would be ""Metallica-Ride the Lighting"

        Args:
            index_name (str): the index column name
            column_names (list): list of columns to concatenate into an index column
        """
        df = self.df
        # use logger to log what the function is attempting to do
        logger.info(f"\tAdding index {index_name}")
        # concatenating the specified column values across the index
        df[index_name] = df[column_names].apply(lambda row:"-".join(row.values.astype(str)), axis=1)
        df.set_index(index_name, inplace=True)
        self.df = df


    def sort(self, column_name:str) -> None:
        """
        Sorts the dataframe by a particular column

        Args:
            column_name (str): column name to sort by
        """
        df = self.df
        df.sort_values(by=column_name)
        

    def load_to_db(self, db_engine, db_table_name:str) -> None:
        """
        Loads the dataframe into a database table.

        Args:
            db_engine (SqlAlchemy Engine): SqlAlchemy engine (or connection) to use to insert into database
            db_table_name (str): name of database table to insert to
        """
        db_host = "127.0.0.1"
        db_user = "root"
        db_pass = "mysql"
        db_engine = db_engine(db_host, db_user, db_pass)

        metadata = sa.MetaData(bind=db_engine)
        logger.info("new metadata")
        conn = db_engine.connect()
        self.engine = db_engine
        self.conn = conn
        self.metadata = metadata



def db_engine(db_host:str, db_user:str, db_pass:str, db_name:str="spotify") -> sa.engine.Engine:
    """Using SqlAlchemy, create a database engine and return it

    Args:
        db_host (str): datbase host and port settings
        db_user (str): database user
        db_pass (str): database password
        db_name (str): database name, defaults to "spotify"

    Returns:
        sa.engine.Engine: sqlalchemy engine
    """
    connection_method = "mysql+pymysql"
    db_port = 3306

    sa.create_engine(f"{connection_method}://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}", future=True)



def db_create_tables(db_engine, drop_first:bool = False) -> None:
    """
    Using SqlAlchemy Metadata class create two spotify tables (including their schema columns and types)
    for **artists** and **albums**.


    Args:
        db_engine (SqlAlchemy Engine): SqlAlchemy engine to bind the metadata to.
        drop_first (bool): Drop the tables before creating them again first. Default to False
    """
    meta = sa.MetaData(bind=db_engine)

    # your code to define tables go in here
    #   - Be careful, some of the columns like album.available_markets are very long. Make sure you give enough DB length for these. ie: 10240 (10kb)

    # your code to drop and create tables go here


def main():
    """
    Pipeline Orchestratation method.

    Performs the following:
    - Creates a DataLoader instance for artists and albums
    - prints the head for both instances
    - Sets artists index to id column
    - Sets albums index to artist_id, name, and release_date
    - Sorts artists by name
    - creates database engine
    - creates database metadata tables/columns
    - loads both artists and albums into database
    """
    pass


if __name__ == '__main__':
    main()