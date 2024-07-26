# Web Processing Service to ingest KRM validated data into backend and provide service

# Web Processing Service for Marine Projects
A PyWPS implementation of the Web Processing Service that enables automation to a data service for validated KRM data

## Install command
conda create --name env_name --file environment.yml

check if databases for pyproj are installed, on anaconda prompt in the correct environment
import pyproj
trans = pyproj.Transformer.from_crs(3358, 6318)

This should not yield an error. If so, try reinstalling pyproj, otherwise reinstall the entire environment.

## Run service commands

conda activate marineprojects_wps

python pywpws.wsgi

## License of PyWPS

[MIT](https://en.wikipedia.org/wiki/MIT_License)

# usage
This project nables a data service for IHM KRM project data that is validated. It provides the option to submit datafiles that will be validated against agreed rules. Users can manually validate the data, submit for test and submit for final release. Final release implies archive previous set, upload new datafile and reset the data service.
