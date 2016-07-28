# Pymongo example project
This project contains a single python script which create, update and delete documents on a mongo database as example.

In order to run the example it is needed python 2.7, pymongo and a running mongodb server. The mongo server must have a People collection on the default test database.
Then, the script can be run with the python interpreter as shown below:
```
python simple_script.py
```
Inside the script you can configure the host and port on the global variables shown here:
```
HOST = "localhost"
PORT = "27017"
```