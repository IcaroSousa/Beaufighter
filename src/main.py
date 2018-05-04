from src.Database import DBConnection
from src.PostgreSqlDump import PostgreSqlDump

if __name__ == '__main__':

    db = DBConnection("PostgreSQL Unicode", host, port, dbName, user, password)
    dbToRestore = DBConnection("PostgreSQL Unicode", host, port, dbName, user, password)

    pgDump = PostgreSqlDump(db, "")

    print("Starting process")

    pgDump.dumpSchema()
    pgDump.dumpData(True)

    pgDump.createUser(dbToRestore)
    pgDump.createDbToRestore(dbToRestore)

    pgDump.restoreSchema(dbToRestore)
    pgDump.restoreData()

    print("Process concluded")
