import inspect
from . interface import PySqlFieldInterface


class FieldTools(object):
    @staticmethod
    def is_db_field(value):
        if inspect.isclass(value):
            return issubclass(value, PySqlFieldInterface)        
        else:
            return issubclass(type(value), PySqlFieldInterface)        
    
    @staticmethod
    def is_field_and_used_in_aggregation_function(value):
        return FieldTools.is_db_field(value) and value.is_used_in_aggregated_function()

    def is_field_and_not_used_in_aggregation_function(value):
        return FieldTools.is_db_field(value) and not value.is_used_in_aggregated_function()
 
