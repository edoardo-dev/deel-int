## req:
[install docker](https://docs.docker.com/desktop/install/)

## docs:
1. [start](https://docs.docker.com/language/python/build-images/)
2. [run img as container](https://docs.docker.com/language/python/run-containers/)
    *docker run --publish [host port]:[container port] python-docker*
3. [db section](https://docs.docker.com/language/python/develop/)

# how to run:
## start application:
1. start Docker Desktop
2. cd into root folder of the project
3. docker-compose -f docker-compose.dev.yml up --build
4. (Optional) Login into Adminer
- System : select *MySQL* ;
- Server : type *mysqldb* ;
- Username : type *root* ;
- Password : type *p@ssw0rd1* ;
- Database : *empty* ;
- Click *login* button.

## in a second terminal call endpoints:
### create db & tables
1. curl http://localhost:8000/initdb
### load files into tables
2. curl http://localhost:8000/load-contracts
3. curl http://localhost:8000/load-invoices
### chech data is in tables (or use Adminer)
4. curl http://localhost:8000/contracts
5. curl http://localhost:8000/invoices