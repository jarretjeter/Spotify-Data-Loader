# _Spotify Dataloader_

#### By _**Jarret Jeter**_

#### _A data pipeline using classes to load to a sql database_

## Technologies Used

* _Python_
* _Pandas_
* _SQL_
* _Beekeeper Studio_
* _Docker_

## Description

_In the main.py file, a DataLoader class is defined with reusable code to read a csv file and perform pandas and sqlalchemy methods on that csv. Then functions are defined to create a sqlalchemy engine and create tables to load into a sql database._

## Setup/Installation Requirements

* _Make sure you have a text editor such as Visual Studio Code installed.
* _Have a running version of Python3.7
* _Install Beekeeper Studio and Docker
* _Clone this repository (https://github.com/jarretjeter/Spotify-Data-Loader.git) onto your local computer from github_
* _Run the start_db.sh script to create a Docker container connected to the MariaDB database_
* _In Beekeeper create a new connection with type MariaDB, Connection Mode: Host and Port, Host: localhost, Port: 3306, user: root, password: mysql and connect_
* _Click the dropdown menu to the left and then click the spotify database_
* _Now you're ready to run the code in the main.py file to and see the results in beekeeper_

## Known Bugs

* _"Unnamed:0" column is still loaded to database even though the column is not defined in the sqlalchemy table schema_

## License

_If you have any questions, please email me at jarretjeter@gmail.com_

[MIT](https://github.com/jarretjeter/Spotify-Data-Loader/blob/main/LICENSE.txt)

Copyright (c) _5/17/2022_ _Jarret Jeter_
