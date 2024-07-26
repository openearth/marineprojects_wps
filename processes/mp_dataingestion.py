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
import configparser
import geopandas as gpd
import boto3
from botocore.exceptions import ClientError


# read config
fc = r"C:\develop\marineprojects_wps\configuration.txt"
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


def mainhandler(bucket_name, key, test):
    """With bucket_name and key the data can be downloaded from S3. It will return some
    metrics of the file

    Args:
        bucket_name (string): S3 bucketname
        key (string):         Key (full path and filename)
        test (boolean):       True, False indicating test version or not

    Returns:
        string : for now with some metrics of the retrieved file
    """
    print(bucket_name)
    print(test)
    # localfile declaration
    localfile = r"C:\develop\marineprojects_wps\geopackage\new.gpkg"

    # get file from s3
    s3fileprocessing(bucket_name, key, localfile)
    print("data downloaded")

    # read file with geopandas
    gdf = gpd.read_file(localfile)

    # derive some stats
    nrrecords = len(gdf)
    nrcolums = len(gdf.columns)

    string = f"File ({key}) is valid geopackage with {nrrecords} of records in {nrcolums} columns"
    return string


def test():
    bucket_name = "krm-validatie-data-floris"
    key = "geopackage/output.gpkg"
    msg = mainhandler(bucket_name, key, True)
    print(msg)
