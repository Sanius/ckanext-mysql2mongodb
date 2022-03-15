class TempDirNotCreatedError(Exception):
    pass


class InvalidFileExtensionError(Exception):
    pass


class DatabaseConnectionError(Exception):
    pass


class UnsupportedDatabaseException(Exception):
    pass


class UnspecifiedDatabaseException(Exception):
    pass
