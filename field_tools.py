import inspect
from interface import PySqlFieldInterface


class FieldTools(object):
    @staticmethod
    def is_db_field(value):
        if inspect.isclass(value):
            return issubclass(value, PySqlFieldInterface)        
        else:
            return issubclass(type(value), PySqlFieldInterface)        
            