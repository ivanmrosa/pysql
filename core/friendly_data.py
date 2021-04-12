import os, sys, datetime

class FriendlyRow(object):
    def __init__(self, fields_names):
        self.fields_names = fields_names
        self.__row = ()
        self.current_field_index = 0
        self.value_as_dict = None
    
    @property
    def row(self):
        return self.__row
    
    @row.setter
    def row(self, value):
        self.__row = value
        self.current_field_index = 0
        self.value_as_dict = None

    def convert_row_to_dict(self):
        value = {}
        index = 0
        for field_name in self.fields_names:
            f = self.row[index]
            if type(f) in (datetime.datetime, datetime.date):
                f = f.isoformat()
            value.update({field_name : f})
            index += 1
        return value

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.current_field_index > len(self.row) - 1:
            raise StopIteration
        else:
            return self.__row[self.current_field_index]

    def __getitem__(self, x):
        if type(x) is int:
            return self.__row[x]
        else:
            if not self.value_as_dict:                
                self.value_as_dict = self.convert_row_to_dict()
            return self.value_as_dict[x]
    

class FriendlyData(object):
    def __init__(self, data_tuple, fields_names):
        if not fields_names:
            raise Exception("Field names must be provided")
        self.data = data_tuple
        self.hight = len(self.data) - 1
        self.current = 0
        self.fields_names = fields_names
        self.friendly_row = FriendlyRow(fields_names)        

    def __len__(self):
        return len(self.data)

    def __getitem__(self, x):
        self.friendly_row.row = self.data[x]
        return self.friendly_row

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.current > self.hight:
            raise StopIteration
        else:
            self.current += 1
            self.friendly_row.row = self.data[self.current - 1]
            return self.friendly_row
    
    def as_dict_list(self):
        result = []
        for r in self.data:
            self.friendly_row.row = r
            result.append(self.friendly_row.convert_row_to_dict())
        
        return result
    
    def get_first(self):
        result = {}
        if len(self.data) > 0:
            self.friendly_row.row = self.data[0]
            result = self.friendly_row.convert_row_to_dict()
        
        return result

