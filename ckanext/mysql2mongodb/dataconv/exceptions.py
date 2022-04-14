class TempDirNotCreatedError(Exception):
    pass


class InvalidFileExtensionError(Exception):
    pass


class DatabaseConnectionError(Exception):
    pass


class UnavailableResourceError(Exception):
    pass


class UploadResourceError(Exception):
    pass


class UnsupportedDatabaseException(Exception):
    pass


class UnspecifiedDatabaseException(Exception):
    pass


class DatatypeMappingException(Exception):
    pass


class MySQLDatabaseNotFoundException(Exception):
    pass


class MongoDatabaseNotFoundException(Exception):
    pass


class MongoCollectionNotFoundException(Exception):
    pass
