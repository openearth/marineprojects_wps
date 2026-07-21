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
import uuid
import configparser
import geopandas as gpd
import logging
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker
import geoalchemy2

logger = logging.getLogger("PYWPS")

SPATIAL_INDEX_NAME = "idx_krm_actuele_dataset_geometry"
APPEND_REINDEX_THRESHOLD = 0.30

# read config
if os.name == "nt":
    fc = r"C:\develop\marineprojects_wps\configuration.txt"
else:
    fc = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configuration.txt")
    if not os.path.exists(fc):
        fc = "/opt/pywps/configuration.txt"
        logger.info("Configuration path fallback selected: %s", fc)
    # print("PG configpath", confpath)
    logger.info("Configuration path selected: %s", fc)

if os.name == "nt":
    logger.info("Configuration path selected: %s", fc)

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
    logger.info(
        "Database engine created for host=%s db=%s",
        cf.get("PostGIS", "host"),
        cf.get("PostGIS", "db"),
    )
    Session = sessionmaker(bind=engine)
    session = Session()
    session.rollback()
    return session, engine


def s3fileprocessing(bucket_name, key, localfile, run_id=None):
    """Downloads file from defined bucket and stores locally

    Args:
        bucket_name (string): S3 bucketname
        key (string):         Key (full path and filename)
        localfile (string):   targetfile to store
    """
    run_token = run_id or "n/a"
    try:
        logger.info(
            "[run_id=%s] Downloading from S3 bucket=%s key=%s to %s",
            run_token,
            bucket_name,
            key,
            localfile,
        )
        s3.Bucket(bucket_name).download_file(key, localfile)
        logger.info("[run_id=%s] S3 download completed: %s", run_token, localfile)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.warning(
                "[run_id=%s] S3 object not found: bucket=%s key=%s",
                run_token,
                bucket_name,
                key,
            )
        else:
            raise


def _table_row_count(engine, schema, table_name, run_id=None):
    """Return the number of rows in a schema-qualified table."""
    strsql = f'SELECT COUNT(*) FROM "{schema}"."{table_name}";'
    with engine.connect() as conn:
        rowcount = conn.execute(text(strsql)).scalar_one()
    if run_id:
        logger.info(
            "[run_id=%s] Existing rows in %s.%s: %s",
            run_id,
            schema,
            table_name,
            rowcount,
        )
    return rowcount


def _index_exists(engine, schema, index_name, run_id=None):
    """Return True when an index exists in the provided schema."""
    strsql = """
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = :schema_name AND indexname = :index_name
        LIMIT 1;
    """
    with engine.connect() as conn:
        found = conn.execute(
            text(strsql),
            {"schema_name": schema, "index_name": index_name},
        ).first()
    if run_id:
        logger.info(
            "[run_id=%s] Index presence check %s.%s: %s",
            run_id,
            schema,
            index_name,
            found is not None,
        )
    return found is not None


def _drop_spatial_index(
    engine,
    schema,
    index_name=SPATIAL_INDEX_NAME,
    concurrently=True,
    run_id=None,
):
    """Drop spatial index with optional CONCURRENTLY outside transaction blocks."""
    conc = " CONCURRENTLY" if concurrently else ""
    strsql = f'DROP INDEX{conc} IF EXISTS "{schema}"."{index_name}";'
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(strsql))
    if run_id:
        logger.info("[run_id=%s] dropped index if present: %s.%s", run_id, schema, index_name)
    else:
        logger.info(f"dropped index if present: {schema}.{index_name}")


def _create_spatial_index(
    engine,
    schema,
    table_name="krm_actuele_dataset",
    geometry_column="geom",
    index_name=SPATIAL_INDEX_NAME,
    concurrently=True,
    run_id=None,
):
    """Create spatial GIST index and refresh planner stats."""
    conc = " CONCURRENTLY" if concurrently else ""
    str_create = (
        f'CREATE INDEX{conc} IF NOT EXISTS "{index_name}" '
        f'ON "{schema}"."{table_name}" USING GIST ("{geometry_column}");'
    )
    str_analyze = f'ANALYZE "{schema}"."{table_name}";'
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(str_create))
        conn.execute(text(str_analyze))
    if run_id:
        logger.info(
            "[run_id=%s] created index and analyzed table: %s.%s",
            run_id,
            schema,
            index_name,
        )
    else:
        logger.info(f"created index and analyzed table: {schema}.{index_name}")

def loaddata2pg_production(gdf, schema, run_id=None):
    """This function creates a table based on the contents of the Geopandas Dataframe
       The function creates a copy of the data based on current datatime
       Production version appends data to original table
    Args:
        gdf (GeoPandas dataframe): geodatafram

    Returns:
        msg (boolean): boolean value indicating success (True) or not (False)
    """
    msg = True
    strmsg = ''
    session, engine = establishconnection(cf)
    run_token = run_id or "n/a"
    logger.info("[run_id=%s] Starting production-style load for schema=%s", run_token, schema)
    try:
       # test if the dataset is already there
        insp = inspect(engine)
        dt = datetime.date.today().strftime("%Y%m%d")
        # check what to do with copy of dataset of same day?
        #print("schema", schema)
        logger.info("[run_id=%s] Processing schema=%s backup_date=%s", run_token, schema, dt)
        if insp.has_table("_".join(["krm_actuele_dataset", dt]), schema=schema):
            strmsg = "copy of table" + schema + "." + "krm_actuele_dataset" + "_" + dt
            logger.info("[run_id=%s] %s", run_token, strmsg)
            strsql = f"""drop table {schema}.krm_actuele_dataset_{dt}"""
            with engine.connect() as conn:
                conn.execute(text(strsql))
                conn.commit()
        else:
            strmsg = "table not found " + schema + "." + "krm_actuele_dataset" + "_" + dt
            logger.info("[run_id=%s] %s", run_token, strmsg)

        # this should always happen, otherwise apparently a new instance has been started
        if insp.has_table("krm_actuele_dataset", schema=schema):
            # rename if true
            strsql = f"""create table {schema}.krm_actuele_dataset_{dt} as select * from {schema}.krm_actuele_dataset"""
            strmsg = "create copy of existing data and create " + schema + "." + "krm_actuele_dataset" + "_" + dt
            logger.info("[run_id=%s] %s", run_token, strmsg)

            logger.info(
                "[run_id=%s] Creating backup table before append: %s.%s",
                run_token,
                schema,
                "_".join(["krm_actuele_dataset", dt]),
            )
            with engine.connect() as conn:
                conn.execute(text(strsql))
                conn.commit()
        else:
            logger.warning(
                "[run_id=%s] Base table missing before append: %s.krm_actuele_dataset",
                run_token,
                schema,
            )

        existing_rows = _table_row_count(engine, schema, "krm_actuele_dataset", run_id=run_token)
        incoming_rows = len(gdf)
        should_reindex = existing_rows > 0 and (
            incoming_rows / existing_rows >= APPEND_REINDEX_THRESHOLD
        )

        if should_reindex:
            logger.info(
                "[run_id=%s] incoming batch is large enough to rebuild index (%s incoming, %s existing)",
                run_token,
                incoming_rows,
                existing_rows,
            )
            _drop_spatial_index(engine, schema, concurrently=True, run_id=run_token)
        else:
            logger.info(
                "[run_id=%s] keeping existing index during append (%s incoming, %s existing)",
                run_token,
                incoming_rows,
                existing_rows,
            )

        # from here the passed GeoPandas dataframe is appended in to the existing table
        # first sanity check on columnname of the geometry column, should be geom
        if 'geometry' in gdf.columns:
            gdf.rename_geometry('geom',inplace=True)
            logger.info("[run_id=%s] Renamed geometry column to geom", run_token)
        
        # check the SRID of the table, needs to match the SRID of the GDF
        checktableSRID(schema, run_id=run_token)
        
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

        # Recreate index only for large appends or when index is missing.
        if should_reindex or not _index_exists(engine, schema, SPATIAL_INDEX_NAME, run_id=run_token):
            _create_spatial_index(engine, schema, concurrently=True, run_id=run_token)
        else:
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(f'ANALYZE "{schema}"."krm_actuele_dataset";'))
            logger.info("[run_id=%s] Kept index and refreshed planner stats for schema=%s", run_token, schema)

        #print("data appended to table, set index GIST on geom")
        logger.info("[run_id=%s] Production-style load finished for schema=%s", run_token, schema)
        session.close()
        engine.dispose()
    except Exception as e:
        logger.exception("[run_id=%s] Production-style load failed for schema=%s: %s", run_token, schema, e)
        msg = False
    return msg


def loaddata2pg_test(gdf, schema, run_id=None):
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
    run_token = run_id or "n/a"
    logger.info("[run_id=%s] Starting replace-style load for schema=%s", run_token, schema)
    try:
        # from here the passed GeoPandas dataframe is inserted in to the database and
        # replaces an existing one!
        # check columnname geom
        if 'geometry' in gdf.columns:
            gdf.rename_geometry('geom',inplace=True)
            logger.info("[run_id=%s] Converted geometry column to geom", run_token)

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
        checktableSRID(schema, run_id=run_token)

        # The replace flow removes indexes; recreate and analyze every run.
        _create_spatial_index(engine, schema, concurrently=False, run_id=run_token)

        # close session and dispose the current engine        
        logger.info("[run_id=%s] Replace-style load finished for schema=%s", run_token, schema)
        session.close()
        engine.dispose()
    except Exception as e:# Log the exception with traceback        
        msg = False
        logger.exception("[run_id=%s] Replace-style load failed for schema=%s: %s", run_token, schema, e)
    return msg

def checktableSRID(schema, srid=4258, run_id=None):
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
        if run_id:
            logger.info(
                "[run_id=%s] Current table SRID for %s.krm_actuele_dataset: %s",
                run_id,
                schema,
                srid,
            )
        else:
            logger.info("Current table SRID for %s.krm_actuele_dataset: %s", schema, srid)
    if srid == 0:
        strsql = f"""select UpdateGeometrySRID('{schema}', 'krm_actuele_dataset', 'geom', {srid})""" 
        conn.execute(text(strsql))
        conn.commit()
        if run_id:
            logger.info("[run_id=%s] Table SRID updated for %s.krm_actuele_dataset", run_id, schema)
        else:
            logger.info("Table SRID updated for %s.krm_actuele_dataset", schema)

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
    run_id = uuid.uuid4().hex[:8]
    schema = "ihm_krm_test"
    if test == "False":
        # bear in mind, this should be changed into ihm_krm, but only after full approval of IHM
        schema = "ihm_krm"

    logger.info("[run_id=%s] Ingestion request received schema=%s test=%s", run_id, schema, test)
    try:
        # localfile declaration
        if os.name == "nt":
            localfile = r"C:\develop\marineprojects_wps\geopackage\new.gpkg"
        else:
            localfile = "/opt/pywps/geopackage/new.gpkg"

        # get file from s3
        s3fileprocessing(bucket_name, key, localfile, run_id=run_id)
        msg = f"data downloaded to {localfile}"
        logger.info("[run_id=%s] %s", run_id, msg)
        

        # read file with geopandas
        # gdf = gpd.read_file(localfile, layer="krm_actuele_dataset")
        gdf = gpd.read_file(localfile)

        # derive some stats
        nrrecords = len(gdf)
        nrcolums = len(gdf.columns)
        gdfcrs = gdf.crs

        # load data in pg
        string = f"File ({localfile}) is valid geopackage with {nrrecords} of records in {nrcolums} columns, with crs {str(gdfcrs)}"
        logger.info("[run_id=%s] %s", run_id, string)
        logger.info("[run_id=%s] Routing load flow based on test flag=%s", run_id, test)
        if test == 'True':
            succeeded = loaddata2pg_test(gdf, schema, run_id=run_id)
            if succeeded:
                string = (
                    string
                    + " loaded in database in test schema (ihm_krm_test), test data service refreshed (ihm_krm_test)"
                )
        elif test == 'False':
            succeeded = loaddata2pg_production(gdf, schema, run_id=run_id)
            if succeeded:
                string = (
                    string + " loaded in production schema, and data service refreshed"
                )
        else:
            logger.warning("[run_id=%s] Unexpected test flag value received: %s", run_id, test)

    except Exception as e:
        logger.exception(
            "[run_id=%s] Main ingestion handler failed bucket=%s key=%s schema=%s: %s",
            run_id,
            bucket_name,
            key,
            schema,
            e,
        )
        string = "downloading file failed"
    finally:
        logger.info("[run_id=%s] %s", run_id, string)
        return string


def mainhandler_dev(bucket_name, key):
    """Dev ingestion handler.

    Downloads the provided GeoPackage from S3, reads it with GeoPandas and loads it into
    the dev schema using the production-style loader (daily backup + append).

    Args:
        bucket_name (string): S3 bucketname
        key (string):         Key (full path and filename)

    Returns:
        string : for now with some metrics of the retrieved file
    """

    run_id = uuid.uuid4().hex[:8]
    schema = "ihm_krm_dev"
    logger.info("[run_id=%s] Dev ingestion request received schema=%s", run_id, schema)
    try:
        # localfile declaration
        if os.name == "nt":
            localfile = r"C:\develop\marineprojects_wps\geopackage\new.gpkg"
        else:
            localfile = "/opt/pywps/geopackage/new.gpkg"

        # get file from s3
        s3fileprocessing(bucket_name, key, localfile, run_id=run_id)
        msg = f"data downloaded to {localfile}"
        logger.info("[run_id=%s] %s", run_id, msg)

        # read file with geopandas
        # gdf = gpd.read_file(localfile, layer="krm_actuele_dataset")
        gdf = gpd.read_file(localfile)

        # derive some stats
        nrrecords = len(gdf)
        nrcolums = len(gdf.columns)
        gdfcrs = gdf.crs

        # load data in pg
        string = f"File ({localfile}) is valid geopackage with {nrrecords} of records in {nrcolums} columns, with crs {str(gdfcrs)}"
        logger.info("[run_id=%s] %s", run_id, string)

        succeeded = loaddata2pg_production(gdf, schema, run_id=run_id)
        if succeeded:
            string = string + " loaded in dev schema, and data service refreshed"

    except Exception as e:
        logger.exception(
            "[run_id=%s] Dev ingestion handler failed bucket=%s key=%s schema=%s: %s",
            run_id,
            bucket_name,
            key,
            schema,
            e,
        )
        string = "downloading file failed"
    finally:
        logger.info("[run_id=%s] %s", run_id, string)
        return string


def test():
    bucket_name = "krm-validatie-data-prod"
    key = "geopackages_history/krm_actuele_dataset_new.gpkg"
    msg = mainhandler(bucket_name, key, "True")
    logger.info(msg)

