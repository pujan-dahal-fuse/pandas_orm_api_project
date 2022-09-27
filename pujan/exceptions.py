class Error(Exception):
    """Base class for custom exceptions"""
    def get_message(self):
        return self.__msg__


class InvalidInput(Error):
    def __init__(self, msg):
        self.__msg__ = msg

class DuplicateValue(Error):
    def __init__(self, msg):
        self.__msg__ = msg

