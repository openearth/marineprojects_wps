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


def establishconnection(fc, connectionsstring=None):
    """
    Set up a orm session to the target database with the connectionstring
    in the file that is passed

    Parameters
    ----------
    fc : string
        DESCRIPTION.
        Location of the file with a connectionstring to a PostgreSQL/PostGIS
        database
    connectionstring:
        DESCRIPTION.
        in case fc = none, a connectionstring can be passed

    Returns
    -------
    session : ormsession
        DESCRIPTION.
        returns orm session

    """
    if fc != None:
        f = open(fc)
        engine = create_engine(f.read(), echo=False)
        f.close()
    elif fc == None and connectionsstring != None:
        engine = create_engine(connectionsstring, echo=False)
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
            print("The object does not exist.")
        else:
            raise


def loaddata2pg(gdf, schema):
    """This function creates a table based on the contents of the Geopandas Dataframe
       The function creates a copy of the data based on current datatime

    Args:
        gdf (GeoPandas dataframe): geodatafram

    Returns:
        msg (boolean): boolean value indicating success (True5) or not (False)
    """
    msg = True

    try:
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
        session, engine = establishconnection(None, connstr)

        # test if the dataset is already there
        insp = inspect(engine)
        dt = datetime.date.today().strftime("%Y%m%d")
        # check what to do with copy of dataset of same day?
        print("schema", schema)
        if insp.has_table("_".join(["krm_actuele_dataset", dt]), schema=schema):
            print("copy of table", schema + "." + "krm_actuele_dataset" + "_" + dt)
            strsql = f"""drop table {schema}.krm_actuele_dataset_{dt}"""
            with engine.connect() as conn:
                conn.execute(text(strsql))
                conn.commit()

        # this should always happen, otherwise apparently a new instance has been started
        if insp.has_table("krm_actuele_dataset", schema=schema):
            # rename if true
            strsql = f"""create table {schema}.krm_actuele_dataset_{dt} as select * from ihm_krm.krm_actuele_dataset"""
            print(
                "create copy of existing data and create",
                schema + "." + "krm_actuele_dataset" + "_" + dt,
            )
            with engine.connect() as conn:
                conn.execute(text(strsql))
                conn.commit()
        # from here the passed GeoPandas dataframe is inserted in to the database and
        # replaces an existing one!
        with engine.connect() as con:
            gdf.to_postgis(
                "krm_actuele_dataset",
                con,
                schema=schema,
                if_exists="replace",
                index=False,
            )
            print("creation of table done")
        session.close()
        engine.dispose()

        checkgeom(engine, ".".join([schema, "krm_actuele_dataset"]))
    except:
        msg = False
    return msg


def checkgeom(engine, tbl):
    """This function creates rename a geometry column to geom (that is expected in geoserver)

    Args:
        engine (sqlalchemey engine object): engine
        tbl (text): table reference (incl. schema)

    Returns:

    """
    strsql = f"""alter table {tbl} rename column geometry to geom"""
    with engine.connect() as conn:
        conn.execute(text(strsql))
        conn.commit()


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
            # localfile = r"C:\develop\marineprojects_wps\geopackage\new_volledig.gpkg"
            # localfile = r"C:\develop\marineprojects_wps\geopackage\new_onvolledig.gpkg"
        else:
            localfile = "./opt/pywps/geopackage/new.gpkg"

        # get file from s3
        s3fileprocessing(bucket_name, key, localfile)
        logging.info("data downloaded to", localfile)
        print("localfile", localfile)
        # read file with geopandas
        # gdf = gpd.read_file(localfile, layer="krm_actuele_dataset")
        gdf = gpd.read_file(localfile)

        # derive some stats
        nrrecords = len(gdf)
        nrcolums = len(gdf.columns)

        # load data in pg
        string = f"File ({key}) is valid geopackage with {nrrecords} of records in {nrcolums} columns"
        print(string)
        logging.info(string)
        if test == "True":
            succeeded = loaddata2pg(gdf, schema)
            if succeeded:
                string = (
                    string
                    + " loaded in database in test schema (ihm_krm_test), test data service refreshed (ihm_krm_test)"
                )
        else:
            # succeeded = loaddata2pg(gdf, schema)
            succeeded = False
            if succeeded:
                string = (
                    string + " loaded in production schema, and data service refreshed"
                )

    except:
        string = "downloading file failed"
    finally:
        return string


# this works, but ...
# implement the test, what is expected there:
# the current setup is such that there are x number of views associated with this
# table, needs to be a testing environment.


def test():
    bucket_name = "krm-validatie-data-floris"
    key = "./geopackage/new.gpkg"
    msg = mainhandler(bucket_name, key, "True")
    print(msg)
