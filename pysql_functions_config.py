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
        return 'TRIM(both {character} from {field})'.format(character=character)

    @staticmethod
    def fltrim(character): 
        return 'TRIM(start {character} from {field})'.format(character=character)

    @staticmethod
    def frtrim(character): 
        return 'TRIM(end ''{character}'' from {field})'.format(character=character)

    @staticmethod
    def flength(): 
        return 'LENGTH({field})'

    @staticmethod
    def freplace(replace_this, replace_to): 
        return 'REPLACE({field}, {replace_this}, {replace_to})'.format(replace_this=replace_this, replace_to=replace_to)
    
    @staticmethod
    def finstr(substring): 
        return 'POSITION({substring} in {field})'.format(substring=substring)
    
    @staticmethod
    def fconcat():
        return ''

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
    def finstr(): 
        return 'INSTR({field}, {substring})'

    @staticmethod
    def fsubstr(inital, length): 
        return 'SUBSTR({field}, {initial}, {length})'.format(field='{field}', initial=inital, length=length)
    @staticmethod
    def ftrim(character): 
        return 'TRIM({field}, {character})'.format(character=character)
    @staticmethod
    def fltrim(character): 
        return 'LTRIM({field}, {character})'.format(character=character)
    @staticmethod
    def frtrim(character): 
        return 'RTRIM({field}, {character})'.format(character=character)
    