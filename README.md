## audev.kennzahlenupdate

This repo builds the foundation for the daily Kennzahlenupdate report. 
It consists of two parts:

* import
* forecast

The _import_ part takes care of importing all necessary data from various 
sources. Build a docker image with 
``docker build -t <tag>:<version> -f import/Dockerfile .``.

The _forecast_ part takes care of the monthly forecast generated from the IVW 
data. Build a docker image with 
``docker build -t <tag>:<version> -f forecast/Dockerfile .``.

Both parts share common code, however they are treated as seperate applications 
to keep the flexibility of executing them standalone.  

