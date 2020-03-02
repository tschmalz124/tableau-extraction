import os, re

class parse_sql_files(object):
    def __init__(self, query_or_path):
        self.query_or_path = query_or_path
        self.tokens = self.get_list_from_object(self.query_or_path)

    def get_basic_tables_and_columns(self):
        tables = self.get_table_names(self.tokens)
        return {'tables' : self.get_columns_names(self.tokens, tables = tables)}

    def get_basic_created_tables(self):
        temp = dict()
        temp['created_tables'] = self.get_created_tables_names(self.tokens)
        return temp

    def get_basic_insert_tables(self):
        temp = dict()
        temp['insert_tables'] = self.get_insert_tables_names(self.tokens)
        return temp

    @classmethod
    def get_list_from_object(cls, input_object):
        try:
            if os.path.exists(input_object):
                with open(input_object, 'r') as f:
                    out_file = f.read()
                    return cls.get_listification(out_file)
            elif isinstance(input_object, str):
                return cls.get_listification(input_object)
            else:

                raise ValueError
        except IsADirectoryError as e:
            print("You just Provided a directory instead of a file.")
        except FileNotFoundError as e:
            print("You just Provided an inexisting file path.")
        except ValueError:
            print("You just Provided neither a file nor a query.")
        except TypeError as e:
            print("Probably you provided a file which is not .sql\n", e)

    @staticmethod
    def get_listification(input_query):
        # remove the /* */ comments
        out_file = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", input_query)
        # detach operators from word strings
        for symbol in [",","="]:
            out_file = re.sub(symbol, ' ' + symbol+ ' ', out_file)
        out_file = out_file.replace('/\s\s+/g', '')
        # remove whole line -- and # comments
        out_file = [line for line in out_file.splitlines() if not re.match("^\s*(--|#)", line)]
        # remove trailing -- and # comments
        out_file = " ".join([re.split("--|#", line)[0] for line in out_file])
        # this line splits everything on blanks, parenthesis, and semicoluns.
        return re.split(r"[\s)(;]+", out_file)

    @classmethod
    def get_columns_names(cls, input_list, **kwargs):
        if kwargs.get('tables'):
            tables = kwargs.get('tables')
        else:
            tables = cls.get_table_names(input_list)
        dictTables = dict()
        for table in tables:
            fields = set()
            for token in input_list:
                if token.startswith(table + '.') or token.startswith(table.split('.')[-1] + '.'):
                    if token != table:
                        fields.add(token)
            if len(list(set(fields))) >= 0:
                dictTables[table] = list(set(fields))
        return dictTables

    @staticmethod
    def get_created_tables_names(input_list):
        tables = set()
        tlen = len(input_list)
        index = 0
        nelem = 2
        while index < (tlen - nelem):
            if input_list[index].lower() in ["create"]:
                if (input_list[(index + 1)].lower() in ["table"] and
                    input_list[(index + 2)].lower() not in ["if"]):
                    tables.add(input_list[(index + 2)]) # get table after from/join
                elif (input_list[(index + 1)].lower() in ["table"] and
                     input_list[(index + 2)].lower() in ["if"]):
                    tables.add(input_list[(index + 5)])
                elif (input_list[(index + 1)].lower() in ["temp","temporary"] and
                     input_list[(index + 2)].lower() in ["table"] and
                     input_list[(index + 3)].lower() not in ["if"]):
                    tables.add(input_list[(index + 3)]) # get cartesian join table
                elif (input_list[(index + 1)].lower() in ["temp","temporary"] and
                     input_list[(index + 2)].lower() in ["table"] and
                     input_list[(index + 3)].lower() in ["if"]):
                    tables.add(input_list[(index + 6)])
            index += 1
        return tables

    @staticmethod
    def get_insert_tables_names(input_list):
        tables = set()
        tlen = len(input_list)
        index = 0
        nelem = 2
        while index < (tlen - nelem):
            if (input_list[index].lower() in ["insert"] and
                input_list[(index + 1)].lower() in ["into"]):
                tables.add(input_list[(index + 2)])
            index += 1
        return tables

    @staticmethod
    def get_table_names(input_list):
        tables = set()
        tlen = len(input_list)
        index = 0
        nelem = 1
        while index < (tlen - nelem):
            if input_list[index].lower() in ["from", "join"]:
                if (input_list[(index + 1)].lower() not in ["","select"]
                    and input_list[(index - 2)].lower() not in ["execept"]):
                    tables.add(input_list[(index + 1)]) # get table after from/join
                elif (input_list[(index + 2)].lower() in [","] and
                    input_list[(index + 3)].lower() not in ["","select"]
                    and input_list[(index - 2)].lower() not in ["execept"]):
                    tables.add(input_list[(index + 3)]) # get cartesian join table
            index += 1
        return tables

#     @classmethod
#     def get_vagabond_columns(input_list):


#     @staticmethod
#     def get_select_from_places(input_list):
#         lsf = []
#         for index, i in enumerate(input_list):

    # additional parts: to test before implementation 1) table alias 2) column alias 3) vagabond columns
    # (column list between select from, without a clear table association).
    # get insert into
