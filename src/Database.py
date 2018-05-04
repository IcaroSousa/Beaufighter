import pyodbc


class DBConnection:

    def __init__(self, pDrive, pHost, pPort, pDatabaseName, pUser, pPassword):
        self.driver = "{" + pDrive + "}"
        self.host = pHost
        self.port = pPort
        self.databaseName = pDatabaseName
        self.user = pUser
        self.password = pPassword

        self.connectionString = "DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={}".format(self.driver,
                                                                                               self.host,
                                                                                               self.port,
                                                                                               self.databaseName,
                                                                                               self.user,
                                                                                               self.password)

        self.connection = None
        if not(self.checkOdbcDriver(pDrive)):
            raise Exception("Driver ODBC -> {} não encontrado, por favor verifique se o mesmo se encontra instalado!".format(pDrive))

    def __checkConnection(self):
        if not self.connection:
            self.connection = pyodbc.connect(self.connectionString)

    def getDriverList(self):
        return pyodbc.drivers()

    def getConnection(self):
        self.__checkConnection()
        return self.connection

    def getQuery(self):
        self.__checkConnection()
        return self.connection.cursor()

    def checkOdbcDriver(self, pDriveName):
        list = self.getDriverList()
        return pDriveName in list
