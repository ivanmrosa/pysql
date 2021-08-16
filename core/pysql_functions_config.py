class PysqlFunctionsConfig(object):
    @staticmethod
    def fsum():
        return 'SUM({field})'

    @staticmethod
    def favg(): 
        return 'AVG({field})'

    @staticmethod
    def fcount(use_distinct=False): 
        if use_distinct:
            return 'COUNT( DISTINCT {field})'
        else:
            return 'COUNT({field})'

    @staticmethod
    def fmax(): 
        return 'MAX({field})'

    @staticmethod
    def fmin(): 
        return 'MIN({field})'

    @staticmethod
    def fupper(): 
        return 'UPPER({field})'

    @staticmethod
    def flower(): 
        return 'LOWER({field})'

    @staticmethod
    def fsubstr(inital, length): 
        return 'SUBSTRING({field} from {initial} for {length})'.format(field= '{field}', initial=inital, length=length)

    @staticmethod
    def ftrim(character): 
        return 'TRIM(both {character} from {field})'.format(character=character, field='{field}')

    @staticmethod
    def fltrim(character): 
        return 'TRIM(LEADING {character} from {field})'.format(character=character, field='{field}')

    @staticmethod
    def frtrim(character): 
        return 'TRIM(TRAILING ''{character}'' from {field})'.format(character=character, field='{field}')

    @staticmethod
    def flength(): 
        return 'LENGTH({field})'

    @staticmethod
    def freplace(replace_this, replace_to): 
        return 'REPLACE({field}, {replace_this}, {replace_to})'.format(replace_this=replace_this, replace_to=replace_to, field='{field}')
    
    @staticmethod
    def finstr(substring): 
        return 'POSITION({substring} in {field})'.format(substring=substring, field='{field}')
    
    @staticmethod
    def fconcat():
        return ''
    
    @staticmethod
    def frpad(complete_with, size):
        return "RPAD({field}, {size}, '{complete_with}' )".format(size=size, complete_with=complete_with, field='{field}')

    @staticmethod
    def flpad(complete_with, size):
        return "LPAD({field}, {size}, '{complete_with}' )".format(size=size, complete_with=complete_with, field='{field}')
    
    @staticmethod
    def pagination():
        return " LIMIT {LIMIT} OFFSET {OFFSET} "  


class PysqlFunctionsConfigPostgre(PysqlFunctionsConfig):
    pass

class PysqlFunctionsConfigOracle(PysqlFunctionsConfig):
    pass

class PysqlFunctionsConfigSqlServer(PysqlFunctionsConfig):
    pass

class PysqlFunctionsConfigMySql(PysqlFunctionsConfig):
    pass

class PysqlFunctionsConfigSqlite(PysqlFunctionsConfig):
    @staticmethod
    def finstr(substring): 
        return 'INSTR({field}, {substring})'.format(substring=substring, field='{field}')

    @staticmethod
    def fsubstr(inital, length): 
        return 'SUBSTR({field}, {initial}, {length})'.format(field='{field}', initial=inital, length=length)

    @staticmethod
    def ftrim(character): 
        return 'TRIM({field}, {character})'.format(character=character, field='{field}' )

    @staticmethod
    def fltrim(character): 
        return 'LTRIM({field}, {character})'.format(character=character, field='{field}')
        
    @staticmethod
    def frtrim(character): 
        return 'RTRIM({field}, {character})'.format(character=character, field='{field}')

    @staticmethod
    def frpad(complete_with, size):
        str_to_add = ''
        for a in range(0, size):
            str_to_add += complete_with
        
        return "{field} || SUBSTR('{complete_with}', 1, {size} - LENGTH({field}) ) ".format(complete_with=str_to_add, field='{field}', size=size)
        

    @staticmethod
    def flpad(complete_with, size):
        str_to_add = ''
        for a in range(0, size):
            str_to_add += complete_with
        
        return "SUBSTR('{complete_with}', 1, {size} - LENGTH({field}) ) || {field} ".format(complete_with=str_to_add, field='{field}', size=size)

