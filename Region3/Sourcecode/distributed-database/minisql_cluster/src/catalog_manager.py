import os
from collections import OrderedDict
import pickle
from functools import lru_cache


class Column:
    def __init__(self, name, fmt, *, primary_key=False, unique=False):
        self.name = name
        self.fmt = fmt
        self.primary_key = primary_key
        self.unique = unique

    def __iter__(self):
        return (x for x in [self.name, self.fmt, self.primary_key, self.unique])


class Index:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = OrderedDict()
        self.indexes = {}
        self.fmt = '<'

    def add_column(self, column):
        self.columns[column.name] = column
        self.fmt += column.fmt

    def add_index(self, index):
        if index.name in self.indexes:
            raise ValueError('already have an index name {} on table {}'.format(index.name,
                                                                                self.name))
        for column in index.columns:
            if column not in self.columns:
                raise ValueError('no column named {} on table {}'.format(column,
                                                                         self.name))
        self.indexes[index.name] = index

    def drop_index(self, index_name):
        if index_name == 'PRIMARY':
            raise ValueError('cannot drop primary index')

        if index_name not in self.indexes:
            raise ValueError('no index named {} on table {}'.format(index_name, self.name))

        del self.indexes[index_name]


class Metadata:
    def __init__(self):
        self.tables = {}

    def dump(self):
        with open('schema/metadata.pickle', 'wb') as file:
            pickle.dump(self, file, protocol=pickle.HIGHEST_PROTOCOL)

    def add_table(self, table_name, *columns):
        if table_name in self.tables:
            raise ValueError('already have a table named {}'.format(table_name))

        if not columns:
            raise ValueError('no columns specified')

        table = Table(table_name)
        primary_keys = []
        for column in columns:
            table.add_column(column)
            if column.primary_key:
                primary_keys.append(column.name)

        if not primary_keys:
            raise ValueError('primary key not specified')

        table.add_index(Index('PRIMARY', primary_keys))
        self.tables[table_name] = table

        # self.dump()

    def drop_table(self, table_name):
        if table_name not in self.tables:
            raise ValueError('no table named {}'.format(table_name))

        del self.tables[table_name]
        # self.dump()

    def add_index(self, table_name, index_name, *column_names):
        if table_name not in self.tables:
            raise ValueError('no table named {}'.format(table_name))
        if not column_names:
            raise ValueError('adding index on empty columns')

        self.tables[table_name].add_index(Index(index_name, column_names))

        # self.dump()

    def drop_index(self, table_name, index_name):
        if table_name not in self.tables:
            raise ValueError('no table named {}'.format(table_name))
        if index_name not in self.tables[table_name].indexes:
            raise ValueError('no index named {} on table {}'.format(index_name, table_name))

        self.tables[table_name].drop_index(index_name)
        # self.dump()


def init():
    os.makedirs('schema/tables', exist_ok=True)


@lru_cache(maxsize=1)
def load_metadata():
    try:
        with open('schema/metadata.pickle', 'rb') as file:
            metadata = pickle.load(file)
            return metadata
    except FileNotFoundError:
        metadata = Metadata()
        with open('schema/metadata.pickle', 'wb') as file:
            pickle.dump(metadata, file)
        return metadata
