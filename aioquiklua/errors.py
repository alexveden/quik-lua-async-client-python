class QuikLuaException(Exception):
    """Base quik-lua exception at RPC/PUB calls"""
    pass


class QuikLuaNoHistoryException(QuikLuaException):
    """Raised when no history returned by Quik"""
    pass


class QuikLuaConnectionException(QuikLuaException):
    """Raised when something happened in underlying network connection"""
    pass
