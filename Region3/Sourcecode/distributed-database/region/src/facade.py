from index_manager import IndexManager
from record_manager import RecordManager
from catalog_manager import load_metadata, Column
from buffer_manager import BufferManager

import os
import shutil


# v1.0 assume each index in built only on a single key now.

# v2.2 use the index
# only support operators in '>, =, <'
# BUGs
# 1. after drop or delete_all operation, the buffer cannot be flushed,
# hence, it's impossible to insert the same record when the old one
# deleted
# 2. cause that we find value by a precise key, which means the key
# must be stored in the B+ tree, we cannot use range_search_with_index
# in general cases. such as we insert 2 records with 'age = 12' and 'age = 50'
# and we have an index on age, it is supported to do search like 'where age = 12'
# or 'where age > 12 and age < 50', but 'where age > 10 and age < 30' is not allowed
#

class MinisqlFacade:
    @staticmethod
    def get_columns_name(table_name):
        metadata = load_metadata()
        columns = list(metadata.tables[table_name].columns.keys())
        return columns

    @staticmethod
    def quit():
        buffer_manager = BufferManager()
        buffer_manager.flush_all()

    @staticmethod
    def create_table(table_name, primary_key, columns):
        os.makedirs('schema/tables/' + table_name, exist_ok=True)
        metadata = load_metadata()  # PK can be set on only one attribute
        columns_lst = []
        for column in columns:
            # generate the fmt for this column
            if column[1][0] == 'char':
                fmt = str(column[1][1]) + 's'
            elif column[1][0] == 'int':
                fmt = str(column[1][1]) + 'i'
            else:
                fmt = str(column[1][1]) + 'd'
            columns_lst.append(Column(column[0], fmt, primary_key=(column[0] == primary_key), unique=column[2]))
        metadata.add_table(table_name, *columns_lst)
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        RecordManager.init_table(table_name)
        metadata.dump()

    @staticmethod
    def insert_record(table_name, attributes):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        metadata = load_metadata()
        position = RecordManager.insert(table_name, metadata.tables[table_name].fmt, tuple(attributes))
        for index_name, index in metadata.tables[table_name].indexes.items():
            file_path = RecordManager.file_dir + index_name + '.index'
            fmt = ''.join(metadata.tables[table_name].columns[column].fmt for column in index.columns)
            manager = IndexManager(file_path, fmt)
            key_pos = list(metadata.tables[table_name].columns.keys()).index(index.columns[0])
            key_list = list()
            key_list.append(attributes[key_pos])
            try:
                manager.insert(key_list, position)  # index can be set on single attribute
                manager.dump_header()
            except ValueError:  # duplicated key, the inserted record should be deleted
                RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=True,
                                     record_offset=position)
                raise

    @staticmethod
    def create_index(table_name, index_name, column_name):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        offset = -1
        metadata = load_metadata()
        metadata.add_index(table_name, index_name, column_name)
        records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False, conditions={})
        file_path = RecordManager.file_dir + index_name + '.index'
        table_target = metadata.tables[table_name]
        fmt = ''.join(table_target.columns[column].fmt for column in table_target.indexes[index_name].columns)
        manager = IndexManager(file_path, fmt)
        key_pos = list(metadata.tables[table_name].columns.keys()).index(column_name)
        for record in records:
            key_list = list()
            key_list.append(record[key_pos])
            offset += 1
            manager.insert(key_list, offset)
        metadata.dump()
        manager.dump_header()

    @staticmethod
    def select_record_all(table_name):
        metadata = load_metadata()
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False, conditions={})
        return records

    @staticmethod
    def _delete_stupid_index(record, table_name):
        metadata = load_metadata()
        for index_name, index in metadata.tables[table_name].indexes.items():
            key_pos = list(metadata.tables[table_name].columns.keys()).index(index.columns[0])
            attribute_name = index.columns[0]
            key_list = list()
            key_list.append(record[key_pos])
            file_path = RecordManager.file_dir + index_name + '.index'
            fmt = metadata.tables[table_name].columns[attribute_name].fmt
            in_manager = IndexManager(file_path, fmt)
            in_manager.delete(key_list)

    @staticmethod
    def delete_record_all(table_name):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        metadata = load_metadata()
        RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=False, conditions={})
        for index_name, index in metadata.tables[table_name].indexes.items():
            file_path = RecordManager.file_dir + index_name + '.index'
            fmt = ''.join(metadata.tables[table_name].columns[column].fmt for column in index.columns)
            manager = IndexManager(file_path, fmt)
            try:
                for i in manager.iter_leaves():
                    key_list = list()
                    key_list.append(i[0][0])
                    manager.delete(key_list)
            except RuntimeError:
                pass

    @staticmethod
    def _convert_conditions(table_name, condition):
        metadata = load_metadata()
        key_pos = list(metadata.tables[table_name].columns.keys()).index(condition[0])
        condition_inter = dict()
        condition_convert = dict()
        condition_inter[condition[1]] = condition[2]
        condition_convert[key_pos] = condition_inter
        return condition_convert

    @staticmethod
    def _convert_conditions_dual(table_name, *conditions):
        metadata = load_metadata()
        if conditions[0][0] == conditions[1][0]:
            key_pos = list(metadata.tables[table_name].columns.keys()).index(conditions[0][0])
            condition_inter = dict()
            condition_convert = dict()
            condition_inter[conditions[0][1]] = conditions[0][2]
            condition_inter[conditions[1][1]] = conditions[1][2]
            condition_convert[key_pos] = condition_inter
            return condition_convert
        else:
            condition_convert = dict()
            for condition in conditions:
                key_pos = list(metadata.tables[table_name].columns.keys()).index(condition[0])
                condition_inter = dict()
                condition_inter[condition[1]] = condition[2]
                condition_convert[key_pos] = condition_inter
            return condition_convert

    @staticmethod
    def select_record_conditionally_without_index(table_name, condition):  # support only equivalent search
        metadata = load_metadata()
        condition_convert = MinisqlFacade._convert_conditions(table_name, condition)
        records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False,
                                       conditions=condition_convert)
        return records

    @staticmethod
    def delete_record_conditionally_without_index(table_name, condition):
        metadata = load_metadata()
        condition_convert = MinisqlFacade._convert_conditions(table_name, condition)
        records = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=False,
                                       conditions=condition_convert)
        for record in records:
            MinisqlFacade._delete_stupid_index(record, table_name)
        RecordManager.delete(table_name, metadata.tables[table_name].fmt, with_index=False,
                             conditions=condition_convert)

    @staticmethod
    def _has_index(attribute_name, table_name):
        metadata = load_metadata()
        for index_name, index in metadata.tables[table_name].indexes.items():
            if attribute_name in index.columns:
                file_path = RecordManager.file_dir + index_name + '.index'
                fmt = metadata.tables[table_name].columns[attribute_name].fmt
                manager = IndexManager(file_path, fmt)
                try:
                    manager.iter_leaves()
                    return index_name
                except RuntimeError:
                    pass
            else:
                pass
        return None

    @staticmethod
    def _select_single_condition(table_name, condition):
        metadata = load_metadata()
        index_name = MinisqlFacade._has_index(condition[0], table_name)
        if index_name:
            print('select with index on ', condition[0])
            attribute_name = condition[0]
            operator = condition[1]
            key_list = list()
            key_list.append(condition[2])
            file_path = RecordManager.file_dir + index_name + '.index'  #
            fmt = metadata.tables[table_name].columns[attribute_name].fmt
            manager = IndexManager(file_path, fmt)
            records = list()
            if operator == '=':
                try:
                    itr = manager.find(key_list)
                    it_key, value = next(itr)
                    if it_key[0] == key_list[0]:
                        records.append(RecordManager.select(table_name,
                                                            metadata.tables[table_name].fmt,
                                                            with_index=True,
                                                            record_offset=value))
                except StopIteration:
                    pass
            elif operator == '>':
                for i in manager.find(key_list):
                    if i[0][0] > key_list[0]:
                        value = i[1]
                        records.append(RecordManager.select(table_name,
                                                            metadata.tables[table_name].fmt,
                                                            with_index=True,
                                                            record_offset=value))
            elif operator == '<':
                for i in manager.iter_leaves():
                    if i[0][0] < key_list[0]:
                        value = i[1]
                        records.append(RecordManager.select(table_name,
                                                            metadata.tables[table_name].fmt,
                                                            with_index=True,
                                                            record_offset=value))
                    else:
                        break
            else:
                pass
        else:
            records = MinisqlFacade.select_record_conditionally_without_index(table_name, condition)

        return records

    @staticmethod
    def select_record_conditionally(table_name, conditions):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        records = list()
        if len(conditions) == 1:
            records += (MinisqlFacade._select_single_condition(table_name, conditions[0]))
        elif len(conditions) == 3:
            record_1 = MinisqlFacade._select_single_condition(table_name, conditions[0])
            record_2 = MinisqlFacade._select_single_condition(table_name, conditions[2])
            if conditions[1] == 'and':
                records = list(set(record_1).intersection(set(record_2)))
            elif conditions[1] == 'or':
                records = list(set(record_1).union(set(record_2)))
            else:
                pass
                # link the records outside
        else:
            pass

        return records

    @staticmethod
    def _delete_single_condition(table_name, condition):
        metadata = load_metadata()
        index_name = MinisqlFacade._has_index(condition[0], table_name)
        if index_name:
            print('delete with index on ', condition[0])
            attribute_name = condition[0]
            operator = condition[1]
            key_list = list()
            key_list.append(condition[2])
            file_path = RecordManager.file_dir + index_name + '.index'  #
            fmt = metadata.tables[table_name].columns[attribute_name].fmt
            manager = IndexManager(file_path, fmt)
            if operator == '=':
                try:
                    itr = manager.find(key_list)
                    it_key, value = next(itr)
                    record = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=True,
                                                  record_offset=value)
                    if it_key[0] == key_list[0]:
                        MinisqlFacade._delete_stupid_index(record, table_name)
                        RecordManager.delete(table_name,
                                             metadata.tables[table_name].fmt,
                                             with_index=True,
                                             record_offset=value)
                except StopIteration:
                    pass
            elif operator == '>':
                for i in manager.find(key_list):
                    if i[0][0] > key_list[0]:
                        value = i[1]
                        record = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=True,
                                                      record_offset=value)
                        MinisqlFacade._delete_stupid_index(record, table_name)
                        RecordManager.delete(table_name,
                                             metadata.tables[table_name].fmt,
                                             with_index=True,
                                             record_offset=value)
            elif operator == '<':
                for i in manager.iter_leaves():
                    if i[0][0] < key_list[0]:
                        value = i[1]
                        record = RecordManager.select(table_name, metadata.tables[table_name].fmt, with_index=True,
                                                      record_offset=value)
                        MinisqlFacade._delete_stupid_index(record, table_name)
                        RecordManager.delete(table_name,
                                             metadata.tables[table_name].fmt,
                                             with_index=True,
                                             record_offset=value)
                    else:
                        break
            manager.dump_header()
        else:
            MinisqlFacade.delete_record_conditionally_without_index(table_name, condition)

    @staticmethod
    def delete_record_conditionally(table_name, conditions):
        RecordManager.set_file_dir('schema/tables/' + table_name + '/')
        metadata = load_metadata()
        if len(conditions) == 1:
            MinisqlFacade._delete_single_condition(table_name, conditions[0])
        elif len(conditions) == 3:
            if conditions[1] == 'and':
                records = MinisqlFacade.select_record_conditionally(table_name, conditions)
                for record in records:
                    for index_name, index in metadata.tables[table_name].indexes.items():
                        file_path = RecordManager.file_dir + index_name + '.index'
                        fmt = ''.join(metadata.tables[table_name].columns[column].fmt for column in index.columns)
                        manager = IndexManager(file_path, fmt)
                        pos = list(metadata.tables[table_name].columns.keys()).index(index.columns[0])
                        key_list = list()
                        key_list.append(record[pos])
                        itr = manager.find(key_list)
                        trash, value = next(itr)
                        manager.delete(key_list)
                        manager.dump_header()
                    RecordManager.delete(table_name,
                                         metadata.tables[table_name].fmt,
                                         with_index=1,
                                         record_offset=value)

            # each tuple has its PRIMARY KEY index
            # actually only support single attribute index
            elif conditions[1] == 'or':
                MinisqlFacade._delete_single_condition(table_name, conditions[0])
                MinisqlFacade._delete_single_condition(table_name, conditions[2])
            else:
                pass
                # link the records outside
        else:
            pass

    @staticmethod
    def drop_table(table_name):
        metadata = load_metadata()
        buffer_manager = BufferManager()
        MinisqlFacade.delete_record_all(table_name)
        shutil.rmtree('schema/tables/' + table_name + '/', True)
        buffer_manager.detach_from_file('schema/tables/' + table_name + '/' + table_name + '.table')
        for index_name in metadata.tables[table_name].indexes:
            buffer_manager.detach_from_file('schema/tables/' + table_name + '/' + index_name + '.index')
        metadata.drop_table(table_name)
        metadata.dump()

    @staticmethod
    def drop_index(index_name):
        metadata = load_metadata()
        for table_name, table in metadata.tables.items():
            if index_name in table.indexes:
                file_path = 'schema/tables/' + table_name + '/' + index_name + '.index'
                fmt = ''.join(
                    metadata.tables[table_name].columns[column].fmt for column in table.indexes[index_name].columns)
                manager = IndexManager(file_path, fmt)
                metadata.drop_index(table_name, index_name)
                try:
                    for i in manager.iter_leaves():
                        key_list = list()
                        key_list.append(i[0][0])
                        manager.delete(key_list)
                except RuntimeError:
                    pass
        metadata.dump()
