#  Copyright notice
#   --------------------------------------------------------------------
#   Copyright (C) 2023 Deltares for RWS Waterinfo Extra
#   Gerrit.Hendriksen@deltares.nl
#
#   This library is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this library.  If not, see <http://www.gnu.org/licenses/>.
#   --------------------------------------------------------------------
#
# This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
# OpenEarthTools is an online collaboration to share and manage data and
# programming tools in an open source, version controlled environment.
# Sign up to recieve regular updates of this function, and to contribute
# your own tools.

import os
import datetime
import configparser
import geopandas as gpd
import logging
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
import geoalchemy2

logger = logging.getLogger("PYWPS")

# read config
if os.name == "nt":
    fc = r"C:\develop\marineprojects_wps\configuration.txt"
else:
    fc = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configuration.txt")
    if not os.path.exists(fc):
        fc = "/opt/pywps/configuration.txt"
        logger.info("path to configuration", fc)
    # print("PG configpath", confpath)
    logger.info("path to configuration", fc)

cf = configparser.ConfigParser()
cf.read(fc)

# initialize connection to s3
s3id = cf.get("s3", "aws_access_key_id")
s3key = cf.get("s3", "aws_secret_access_key")
s3region = cf.get("s3", "region_name")

s3 = boto3.resource(
    "s3",
    aws_access_key_id=f"{s3id}",
    aws_secret_access_key=f"{s3key}",
    region_name=f"{s3region}",
)


def establishconnection(cf):
    """
    Set up an orm session to the target database with the connectionstring
    in the file that is passed

    Parameters
    ----------
    fc : string
        DESCRIPTION.
        Location of the file with a connectionstring to a PostgreSQL/PostGIS
        database
    connectionstring:
        DESCRIPTION.

    Returns
    -------
    session : ormsession
        DESCRIPTION.
        returns orm session and engine

    """
    connstr = (
        "postgresql+psycopg2://"
        + cf.get("PostGIS", "user")
        + ":"
        + cf.get("PostGIS", "pass")
        + "@"
        + cf.get("PostGIS", "host")
        + ":5432/"
        + cf.get("PostGIS", "db")
    ) 
    engine = create_engine(connstr, echo=False)
    logger.info("connection setup")
    Session = sessionmaker(bind=engine)
    session = Session()
    session.rollback()
    return session, engine


def s3fileprocessing(bucket_name, key, localfile):
    """Downloads file from defined bucket and stores locally

    Args:
        bucket_name (string): S3 bucketname
        key (string):         Key (full path and filename)
        localfile (string):   targetfile to store
    """
    try:
        s3.Bucket(bucket_name).download_file(key, localfile)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info("The object does not exist.")
        else:
            raise

def loaddata2pg_production(gdf, schema):
    """This function creates a table based on the contents of the Geopandas Dataframe
       The function creates a copy of the data based on current datatime
       Production version appends data to original table
    Args:
        gdf (GeoPandas dataframe): geodatafram

    Returns:
        msg (boolean): boolean value indicating success (True5) or not (False)
    """
    msg = True
    strmsg = ''
    session, engine = establishconnection(cf)
    logger.info('gdf passed to production')
    try:
       # test if the dataset is already there
        insp = inspect(engine)
        dt = datetime.date.today().strftime("%Y%m%d")
        # check what to do with copy of dataset of same day?
        #print("schema", schema)
        logging.info('schema is',schema)
        if insp.has_table("_".join(["krm_actuele_dataset", dt]), schema=schema):
            strmsg = "copy of table" + schema + "." + "krm_actuele_dataset" + "_" + dt
            logger.info(strmsg)
            strsql = f"""drop table {schema}.krm_actuele_dataset_{dt}"""
            with engine.connect() as conn:
                conn.execute(text(strsql))
                conn.commit()
        else:
            strmsg = "table not found " + schema + "." + "krm_actuele_dataset" + "_" + dt
            logger.info(strmsg)

        # this should always happen, otherwise apparently a new instance has been started
        if insp.has_table("krm_actuele_dataset", schema=schema):
            # rename if true
            strsql = f"""create table {schema}.krm_actuele_dataset_{dt} as select * from ihm_krm.krm_actuele_dataset"""
            strmsg = "create copy of existing data and create "+ schema + "." + "krm_actuele_dataset" + "_" + dt,
            logger.info(strmsg)

            logging.info("create copy of existing data and create ", schema + "." + "krm_actuele_dataset" + "_" + dt)
            with engine.connect() as conn:
                conn.execute(text(strsql))
                conn.commit()

            session.execute(text("COMMIT"))
            strsql = 'drop index CONCURRENTLY if exists idx_krm_actuele_dataset_geometry;' 
            session.execute(text(strsql))
            strmsg = 'Dropping GIST Index if exists'
            logger.info(strmsg)
        else:
            logger.info('this message should not be there, it means that the table krm_actuele_dataset is not there!') 

        # from here the passed GeoPandas dataframe is appended in to the existing table
        # first sanity check on columnname of the geometry column, should be geom
        if 'geometry' in gdf.columns:
            gdf.rename_geometry('geom',inplace=True)
        
        # check the SRID of the table, needs to match the SRID of the GDF
        checktableSRID(schema)
        
        # replace all textvalues 'nan' to null
        gdf = gdf.replace({'nan': None})
        
        # load geodataframe in postgis
        gdf.to_postgis(
            "krm_actuele_dataset",
            engine,
            schema=schema,
            if_exists="append",
            index=False,
        )

        # set index with GIST on geom column
        #session.execute(text("COMMIT"))
        strsql = f'CREATE INDEX idx_krm_actuele_dataset_geometry ON {schema}.krm_actuele_dataset USING GIST (geom);' 
        #session.execute(text(strsql))        
        with engine.connect() as conn:
            conn.execute(text(strsql))
            conn.commit()

        #print("data appended to table, set index GIST on geom")
        logging.info("creation of table done in schema", schema)
        session.close()
        engine.dispose()
    except Exception as e:
        logger.error(f'Exception raised: {str(e)}')
        logger.exception("Full traceback:") 
        msg = False
    return msg


def loaddata2pg_test(gdf, schema):
    """This function creates a table based on the contents of the Geopandas Dataframe
       The function creates a copy of the data based on current datatime
       Test version only replaces data 
    Args:
        gdf (GeoPandas dataframe): geodatafram

    Returns:
        msg (boolean): boolean value indicating success (True5) or not (False)
    """
    msg = True
    session, engine = establishconnection(cf)
    logger.info('gdf passed to test schema')
    try:
        # from here the passed GeoPandas dataframe is inserted in to the database and
        # replaces an existing one!
        session.execute(text("COMMIT"))
        strsql = 'drop index CONCURRENTLY if exists idx_krm_actuele_dataset_geometry;' 
        session.execute(text(strsql))
        logger.info('loaddata2pg_test: index dropped')
        # check columnname geom
        if 'geometry' in gdf.columns:
            gdf.rename_geometry('geom',inplace=True)
            logger.info('loaddata2pg_test: converted geometry to geom')

        # replace all textvalues 'nan' to null
        gdf = gdf.replace({'nan': None})

        # load geodataframe in postgis
        gdf.to_postgis(
            "krm_actuele_dataset",
            engine,
            schema=schema,
            if_exists="replace",
            index=False,
        )

        #checks the srid of the entire table and sets if necessary
        checktableSRID(schema)

        # close session and dispose the current engine        
        logging.info("loaddata2pg_test: creation of table done in schema")
        session.close()
        engine.dispose()
    except Exception as e:# Log the exception with traceback        
        msg = False
        logger.exception("An unexpected error occurred: %s", e)
        logger.info(f'loaddata2pg_test fout: {e}')
    return msg

def checktableSRID(schema, srid=4258):
    """This function renames a set the entire table to a given srid (defaults to 4258)

    Args:
        schema (string): target schema
        srid (integer) : EPSG code of the spatial reference ID, defaults to 4258
    Returns:
    """

    # setup connection to the database
    session, engine = establishconnection(cf)

    # check srid of target table
    strsql = f"""select find_srid('{schema}', 'krm_actuele_dataset', 'geom')""" 
    with engine.connect() as conn:
        srid = conn.execute(text(strsql)).fetchone()[0]
        conn.commit()
        logger.info(f'database table {srid}')
    if srid == 0:
        strsql = f"""select UpdateGeometrySRID('{schema}', 'krm_actuele_dataset', 'geom', {srid})""" 
        conn.execute(text(strsql))
        conn.commit()
        logger.info('database table set to srid 4258')

    # close session and dispose the current engine
    session.close()
    engine.dispose()
    return

def mainhandler(bucket_name, key, test):
    """With bucket_name and key the data can be downloaded from S3. It will return some
    metrics of the file.
    With test = 'True' then the data will be loaded into test schema (ihm_krm_test) and refreshed in the geoserver
    stora ihm_krm_test. The layers in the geoserver are not advertised (so not visible in layer preview window (except when logged in as admin))

    Args:
        bucket_name (string): S3 bucketname
        key (string):         Key (full path and filename)
        test (boolean):       True, False indicating test version or not

    Returns:
        string : for now with some metrics of the retrieved file
    """
    schema = "ihm_krm_test"
    if test == "False":
        # bear in mind, this should be changed into ihm_krm, but only after full approval of IHM
        schema = "ihm_krm"

    logging.info("schema is ", schema)
    try:
        # localfile declaration
        if os.name == "nt":
            localfile = r"C:\develop\marineprojects_wps\geopackage\new.gpkg"
        else:
            localfile = "/opt/pywps/geopackage/new.gpkg"

        # get file from s3
        s3fileprocessing(bucket_name, key, localfile)
        msg = f"data downloaded to {localfile}"
        logger.info(msg)
        

        # read file with geopandas
        # gdf = gpd.read_file(localfile, layer="krm_actuele_dataset")
        gdf = gpd.read_file(localfile)

        # derive some stats
        nrrecords = len(gdf)
        nrcolums = len(gdf.columns)
        gdfcrs = gdf.crs

        # load data in pg
        string = f"File ({localfile}) is valid geopackage with {nrrecords} of records in {nrcolums} columns, with csr {str(gdfcrs)}"
        logger.info(string)
        logger.info(f'the value of test is {test}')
        if test == 'True':
            succeeded = loaddata2pg_test(gdf, schema)
            if succeeded:
                string = (
                    string
                    + " loaded in database in test schema (ihm_krm_test), test data service refreshed (ihm_krm_test)"
                )
        elif test == 'False':
            succeeded = loaddata2pg_production(gdf, schema)
            if succeeded:
                string = (
                    string + " loaded in production schema, and data service refreshed"
                )
        else:
            logger.info('value of test',test)

    except:
        string = "downloading file failed"
    finally:
        logger.info(string)
        return string


def test():
    bucket_name = "krm-validatie-data-prod"
    key = "geopackages_history/krm_actuele_dataset_new.gpkg"
    msg = mainhandler(bucket_name, key, "True")
    logger.info(msg)

