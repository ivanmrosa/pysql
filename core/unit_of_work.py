from . pysql_class_generator import PySqlClassGenerator

class UnitOFWork:
        

    @classmethod
    def discart(cls):
        PySqlClassGenerator.get_script_executor().rollback()
    
    @classmethod
    def save(cls):        
        PySqlClassGenerator.get_script_executor().commit()
    
