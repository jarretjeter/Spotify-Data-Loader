from importlib_metadata import metadata
import pandas as pd
import sqlalchemy as sa
import logging
from logging import INFO
import sys

# configure logger for helpful debugging messages
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
        # assign this class instance variable to the dataframe 'df'
        self.df = df

    def head(self) -> None:
        """
        prints the head of the dataframe to console
        """
        # assign 'df' variable of this specific method to the instance's dataframe
        df = self.df
        return df.head()

    def info(self):
        """
        calls pandas.info on the dataframe
        """
        df = self.df
        return df.info()

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
        logger.info(f"\tAdding index '{index_name}'")
        # concatenating the specified column values across the index
        df[index_name] = df[column_names].apply(lambda row:"-".join(row.values.astype(str)), axis=1)
        # set newly created index name as the dataframe's index
        df.set_index(index_name, inplace=True)
        self.df = df


    def sort(self, column_name:str) -> None:
        """
        Sorts the dataframe by a particular column

        Args:
            column_name (str): column name to sort by
        """
        df = self.df
        # sort by dataframe column values
        return df.sort_values(by=column_name)
        

    def load_to_db(self, db_engine, db_table_name:str) -> None:
        """
        Loads the dataframe into a database table.

        Args:
            db_engine (SqlAlchemy Engine): SqlAlchemy engine (or connection) to use to insert into database
            db_table_name (str): name of database table to insert to
        """
        
        df = self.df
        logger.info(f"Loading {db_table_name} to database...")
        # loads the sql tables created from the dataframe into a database using a sqlalchemy engine, replacing any tables that already exist
        df.to_sql(db_table_name, db_engine, if_exists="replace")




def db_engine(db_host:str, db_user:str, db_pass:str, db_name:str="spotify") -> sa.engine.Engine:
    """Using SqlAlchemy, create a database engine and return it

    Args:
        db_host (str): database host and port settings
        db_user (str): database user
        db_pass (str): database password
        db_name (str): database name, defaults to "spotify"

    Returns:
        sa.engine.Engine: sqlalchemy engine
    """
    connection_method = "mysql+pymysql"
    # create a sqlalchemy engine, assigning to variable 'engine'
    engine = sa.create_engine(f"{connection_method}://{db_user}:{db_pass}@{db_host}/{db_name}", future=True)
    return engine


def db_create_tables(db_engine, drop_first:bool = False) -> None:
    """
    Using SqlAlchemy Metadata class create two spotify tables (including their schema columns and types)
    for **artists** and **albums**.


    Args:
        db_engine (SqlAlchemy Engine): SqlAlchemy engine to bind the metadata to.
        drop_first (bool): Drop the tables before creating them again first. Default to False
    """
    # create a sqlalchemy MetaData object to hold the information/structure of the tables to be created, and bind it to a sqlalchemy engine to send the information to a database
    meta = sa.MetaData(bind=db_engine)
    

    logger.info("Creating spotify_artists table")
    # defining the table name and its structure and columns
    spotify_artists_table = sa.Table("spotify_artists", meta,
                                sa.Column("id", sa.String(256), primary_key=True),
                                sa.Column("name", sa.String(256)),
                                sa.Column("artist_popularity", sa.Integer),
                                sa.Column("followers", sa.String(256)),
                                sa.Column("genres", sa.String(256)),
                                sa.Column("track_id", sa.String(256)),
                                sa.Column("track_id_prev", sa.String(256)),
                                sa.Column("type", sa.String(256)))


    logger.info("Creating spotify_albums table")
    spotify_albums_table = sa.Table("spotify_albums", meta,
                                sa.Column("id", sa.String(256), primary_key=True),
                                sa.Column("name", sa.String(256)),
                                sa.Column("album_type", sa.String(256)),
                                sa.Column("artist_id", sa.String(256)),
                                sa.Column("available_markets", sa.String(1000)),
                                sa.Column("external_urls", sa.String(1000)),
                                sa.Column("href", sa.String(1000)),
                                sa.Column("images", sa.String(1000)),
                                sa.Column("release_date", sa.String(256)),
                                sa.Column("release_date_precision", sa.String(256)),
                                sa.Column("total_tracks", sa.Integer),
                                sa.Column("track_id", sa.String(256)),
                                sa.Column("track_name_prev", sa.String(256)),
                                sa.Column("uri", sa.String(256)),
                                sa.Column("type", sa.String(256)))

                                
    if drop_first:
        logger.info("Dropping existing tables before creating new ones")
        meta.drop_all()
    logger.info("create_all()")
    # using the metadata, create all the specified tables in the database that the engine is connected to
    meta.create_all(db_engine, checkfirst=True)
    # log the names of the newly created tables
    logger.info(meta.tables.keys())

def main():
    """
    Pipeline Orchestration method.

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
    # reading csv files into a dataframe with the DataLoader class
    artists_df = DataLoader("./data/spotify_artists.csv")
    albums_df = DataLoader("./data/spotify_albums.csv")

    # performing pandas methods on the loaded dataframes
    artists_df.head()
    albums_df.head()
    # here, the artists dataframe's 'id' column is set to the index
    artists_df.add_index("id", ["id"])
    # here, a new index, 'album' is set combining the values of columns: 'name', 'artist_id', and 'release_date'
    albums_df.add_index("album", ["name","artist_id", "release_date"])
    artists_df.sort("name")

    # creating engine to be used
    engine = db_engine("127.0.0.1:3306", "root", "mysql")
    # open the engine connection
    engine.connect()
    db_create_tables(engine, drop_first=True)
    # add engine to artists_df class instance
    artists_df.engine = engine
    # load csv file to sql database with matching schema
    artists_df.load_to_db(engine, "spotify_artists")
    # add engine to albums_df class instance
    albums_df.engine = engine
    albums_df.load_to_db(engine, "spotify_albums")

if __name__ == '__main__':
    main()