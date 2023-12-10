from os import environ

def get_mongo_conn_url() -> tuple[str,dict]:
    """Form connection URL and options from env for connecting to Mongo"""

    if environ.get("env") == "local":
        srv = "".join([environ.get("MONGO_PREFIX"), environ.get("MONGO_HOST")])
        return (srv, dict())

    srv = "".join([environ.get("MONGO_PREFIX"), environ.get("MONGO_USER"), ":", environ.get("MONGO_PASS"), "@", environ.get("MONGO_HOST")])

    options = {}
    if environ.get("MONGO_RS_NAME") != None and len(environ.get("MONGO_RS_NAME")) > 0:
        options['replicaSet'] = environ.get("MONGO_RS_NAME")
    if environ.get("MONGO_DB") != None and len(environ.get("MONGO_DB")) > 0:
        options['authSource'] = environ.get("MONGO_DB")
    return (srv, options)

def get_rds_conn_url() -> str:
    """Form connection URL from env for connecting to Postgresql"""
    
    if environ.get("PGSQL_DB_PORT") == None or environ.get("PGSQL_DB_PORT") == '':
        return "postgresql://{}:{}@{}/{}".format(
            environ.get("PGSQL_DB_USER"),
            environ.get("PGSQL_DB_PASS"),
            environ.get("PGSQL_DB_HOST"),
            environ.get("PGSQL_DB_NAME")
        )
    else:
        return "postgresql://{}:{}@{}:{}/{}".format(
            environ.get("PGSQL_DB_USER"),
            environ.get("PGSQL_DB_PASS"),
            environ.get("PGSQL_DB_HOST"),
            environ.get("PGSQL_DB_PORT"),
            environ.get("PGSQL_DB_NAME")
        )


def get_redis_creds() -> tuple[str,str,str]:
    """Get Redis credentials from env"""

    host = environ.get("REDIS_HOST")
    port = environ.get("REDIS_PORT")
    password = environ.get("REDIS_PASS")
    return host, port, password