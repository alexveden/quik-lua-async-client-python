class QuikLuaException(Exception):
    """Base quik-lua exception at RPC/PUB calls"""


class QuikLuaNoHistoryException(QuikLuaException):
    """Raised when no history returned by Quik"""


class QuikLuaConnectionException(QuikLuaException):
    """Raised when something happened in underlying network connection"""
