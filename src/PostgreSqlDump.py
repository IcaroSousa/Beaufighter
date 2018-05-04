import os
from os import path
from subprocess import Popen as processOpen, PIPE


class PostgreSqlDump:

    def __init__(self, pDatabase, pWorkingPath):
        self.db = pDatabase
        self.workingPath = pWorkingPath

        if pWorkingPath is None or pWorkingPath == "":
            self.workingPath = os.getcwd()

        self.workingPath = path.join(self.workingPath, "Dump")

        if not path.exists(self.workingPath):
            os.mkdir(self.workingPath)

    def __executeCommand(self, pCommand, pShowLog=False):
        pCommand = pCommand.split(" ")

        process = processOpen(pCommand, shell=False, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        result = process.communicate()

        if pShowLog and result:
            if result[0]:
                print("Output => {}".format(result[0]))

            if result[1]:
                print("Error => {}".format(result[1]))

    def __getTables(self):
        _query = self.db.getQuery()
        _query.execute("SELECT '-t ' || nmentidade FROM saj.epadentidadecfg WHERE defiltro IS NULL")

        _data = _query.fetchall()
        _tables = ""
        for item in _data:
            _tables = "{} {}".format(_tables, item[0])

        return _tables

    def dumpSchema(self):
        _command = "pg_dump -d postgresql://{}:{}@{}:{}/{} -F t -s -f {}\{}Schema.tar".format(self.db.user,
                                                                                              self.db.password,
                                                                                              self.db.host,
                                                                                              self.db.port,
                                                                                              self.db.databaseName,
                                                                                              self.workingPath,
                                                                                              self.db.databaseName)

        self.__executeCommand(_command, True)

    def dumpData(self, pFullDump=False):
        _tables = ""
        if not pFullDump:
            _tables = self.__getTables()

        _command = "pg_dump -d postgresql://{}:{}@{}:{}/{} -F t -a{} -f {}\{}Data.tar".format(self.db.user,
                                                                                              self.db.password,
                                                                                              self.db.host,
                                                                                              self.db.port,
                                                                                              self.db.databaseName,
                                                                                              _tables,
                                                                                              self.workingPath,
                                                                                              self.db.databaseName)

        self.__executeCommand(_command, True)

    def createDbToRestore(self, pDatabase):
        _command = "createdb -h {} -p {} -U {} {}".format(pDatabase.host,
                                                          pDatabase.port,
                                                          pDatabase.user,
                                                          pDatabase.databaseName)

        self.__executeCommand(_command)

    def createUser(self, pDatabase):
        _command = "createuser -U postgres -d -l -s -h -r -h {} -p {} {}".format(pDatabase.host,
                                                                                 pDatabase.port,
                                                                                 pDatabase.user)

        self.__executeCommand(_command)

    def restoreSchema(self, pDestinyDb):
        _command = "pg_restore -d postgresql://{}:{}@{}:{}/{} -s -Ft -C {}\{}Schema.tar".format(pDestinyDb.user,
                                                                                                pDestinyDb.password,
                                                                                                pDestinyDb.host,
                                                                                                pDestinyDb.port,
                                                                                                pDestinyDb.databaseName,
                                                                                                self.workingPath,
                                                                                                pDestinyDb.databaseName)

        self.__executeCommand(_command)

    def restoreData(self, pDestinyDb):
        _command = "pg_restore -d postgresql://{}:{}@{}:{}/{} -a -Ft {}\{}Data.tar".format(pDestinyDb.user,
                                                                                           pDestinyDb.password,
                                                                                           pDestinyDb.host,
                                                                                           pDestinyDb.port,
                                                                                           pDestinyDb.databaseName,
                                                                                           self.workingPath,
                                                                                           pDestinyDb.databaseName)

        self.__executeCommand(_command)
