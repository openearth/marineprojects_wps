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

import json
from pywps import Format
from pywps.app import Process
from pywps.inout.outputs import LiteralOutput
from pywps.inout.inputs import ComplexInput, LiteralInput
from pywps.app.Common import Metadata
from .mp_dataingestion import mainhandler


# http://localhost:5000/wps?service=wps&request=GetCapabilities&version=2.0.0
# http://localhost:5000/wps?request=GetCapabilities&service=WPS&version=2.0.0

# example requests
# In case of test
# http://localhost:5000/wps?request=Execute&service=WPS&identifier=wps_mp_dataingestion&version=2.0.0&DataInputs=s3_inputs={"bucketname": "krm-validatie-data-floris","key": "geopackage/output.gpkg","test": "True"}

# In case of acceptence
# http://localhost:5000/wps?request=Execute&service=WPS&identifier=wps_mp_dataingestion&version=2.0.0&DataInputs=s3_inputs={"bucketname": "krm-validatie-data-floris","key": "geopackage/output.gpkg","test": "False"}

# http://localhost:5000/wps?service=wps&request=GetCapabilities&version=2.0.0

# production environment
# https://marineprojects.openearth.nl/wps?service=wps&version=1.0.0&request=GetCapabilities


class WPSMPDataIngestion(Process):
    def __init__(self):
        inputs = [
            ComplexInput(
                "s3_inputs",
                "S3 Inputs - Bucketname, Key and Test (boolean)",
                [Format("application/json")],
                abstract="Complex input abstract",
            )
        ]
        outputs = [
            LiteralOutput(
                "Preview",
                "The provided dataset following statistics",
                data_type="string",
            )
        ]

        super(WPSMPDataIngestion, self).__init__(
            self._handler,
            identifier="wps_mp_dataingestion",
            version="1.3.3.7",
            title="Data ingestion for MarineProjects KRM ",
            abstract="The process accepts an S3 file ID and a boolean value that indicates if the S3 file id is subject to test or bring to production",
            profile="",
            metadata=[
                Metadata("KRM Validation data ingestion"),
                Metadata(
                    "KRM Validation data ingestion with indication of test yes or no"
                ),
            ],
            inputs=inputs,
            outputs=outputs,
            store_supported=False,
            status_supported=False,
        )

    def _handler(self, request, response):

        # Read input
        s3_jsoninput = request.inputs["s3_inputs"][0].data

        # parse input to json and read
        s3data = json.loads(s3_jsoninput)
        bucketname = s3data["bucketname"]
        key = s3data["key"]
        test = s3data["test"]
        # call main handler

        # provide feedback
        res = mainhandler(bucketname, key, test)
        response.outputs["Preview"].data = json.dumps(res)
        return response
