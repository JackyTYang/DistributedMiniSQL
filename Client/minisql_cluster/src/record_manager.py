from minisql_cluster.src.buffer_manager import BufferManager, pin
from struct import Struct
import os


def convert_str_to_bytes(attributes):
    attr_list = list(attributes)
    for index, item in enumerate(attr_list):
        if isinstance(item, str):
            attr_list[index] = item.encode('ASCII')
    return tuple(attr_list)


def convert_bytes_to_str(attributes):
    attr_list = list(attributes)
    for index, item in enumerate(attr_list):
        if isinstance(item, bytes):
            attr_list[index] = item.decode('ASCII').rstrip('\00')
    return tuple(attr_list)


class Record:
    # The format of header should be the same for all records files.
    header_format = '<ii'  # will be confirmed by RecordManager
    header_struct = Struct(header_format)

    def __init__(self, file_path, fmt):
        self.buffer_manager = BufferManager()
        self.filename = file_path
        # Each record in file has 2 extra info: next's record_off and valid bit
        self.record_struct = Struct(fmt + 'ci')
        self.first_free_rec, self.rec_tail = self._parse_header()

    def insert(self, attributes):
        """Insert the given record"""
        record_info = convert_str_to_bytes(attributes) + (b'1', -1)  # valid bit, next free space
        self.first_free_rec, self.rec_tail = self._parse_header()
        if self.first_free_rec >= 0:  # There are space in free list
            first_free_blk, local_offset = self._calc(self.first_free_rec)
            block = self.buffer_manager.get_file_block(self.filename, first_free_blk)
            with pin(block):
                data = block.read()
                records = self._parse_block_data(data, first_free_blk)
                next_free_rec = records[self.first_free_rec][-1]
                records[local_offset] = record_info
                new_data = self._generate_new_data(records, first_free_blk)
                block.write(new_data)
            position = self.first_free_rec
            self.first_free_rec = next_free_rec
        else:  # No space in free list, append the new record to the end of file
            self.rec_tail += 1
            block_offset, local_offset = self._calc(self.rec_tail)
            block = self.buffer_manager.get_file_block(self.filename, block_offset)
            with pin(block):
                data = block.read()
                records = self._parse_block_data(data, block_offset)
                records.append(record_info)
                new_data = self._generate_new_data(records, block_offset)
                block.write(new_data)
            position = self.rec_tail
        self._update_header()
        return position

    def remove(self, record_offset):
        """Remove the record at specified position and update the free list"""
        self.first_free_rec, self.rec_tail = self._parse_header()
        block_offset, local_offset = self._calc(record_offset)
        block = self.buffer_manager.get_file_block(self.filename, block_offset)
        with pin(block):
            data = block.read()
            records = self._parse_block_data(data, block_offset)
            try:
                records[local_offset][-1]
            except IndexError:
                raise IndexError('The offset points to an empty space')
            if records[local_offset][-2] == b'0':
                raise RuntimeError('Cannot remove an empty record')
            records[local_offset][-1] = self.first_free_rec  # A positive number, putting this position into free list
            records[local_offset][-2] = b'0'
            self.first_free_rec = record_offset  # update the head of free list
            new_data = self._generate_new_data(records, block_offset)
            block.write(new_data)
        self._update_header()

    def modify(self, attributes, record_offset):
        """Modify the record at specified offset"""
        block_offset, local_offset = self._calc(record_offset)
        block = self.buffer_manager.get_file_block(self.filename, block_offset)
        record_info = convert_str_to_bytes(attributes) + (b'1', -1)  # Updated record must be real
        with pin(block):
            data = block.read()
            records = self._parse_block_data(data, block_offset)
            if records[local_offset][-2] == b'0':
                raise RuntimeError('Cannot update an empty record')
            records[local_offset] = record_info
            new_data = self._generate_new_data(records, block_offset)
            block.write(new_data)

    def read(self, record_offset):
        """ Return the record at the corresponding position """
        block_offset, local_offset = self._calc(record_offset)
        block = self.buffer_manager.get_file_block(self.filename, block_offset)
        with pin(block):
            data = block.read()
            records = self._parse_block_data(data, block_offset)
            if records[local_offset][-2] == b'0':
                raise RuntimeError('Cannot read an empty record')
        return convert_bytes_to_str(tuple(records[local_offset][:-2]))

    def scanning_select(self, conditions):
        # condition should be a dict: { attribute offset : {operator : value } }
        total_blk = self._calc(self.rec_tail)[0] + 1
        result_set = []
        for block_offset in range(total_blk):
            block = self.buffer_manager.get_file_block(self.filename, block_offset)
            records = self._parse_block_data(block.read(), block_offset)
            result_set += tuple([convert_bytes_to_str(record[:-2]) for record in records
                                 if self._check_condition(record, conditions) is True])
        return result_set

    def scanning_delete(self, conditions):
        total_blk = self._calc(self.rec_tail)[0] + 1
        record_offset = 0
        for block_offset in range(total_blk):
            block = self.buffer_manager.get_file_block(self.filename, block_offset)
            records = self._parse_block_data(block.read(), block_offset)
            for i, record in enumerate(records):
                if self._check_condition(convert_bytes_to_str(record), conditions):
                    records[i][-2] = b'0'
                    records[i][-1] = self.first_free_rec
                    self.first_free_rec = record_offset
                record_offset += 1
            block.write(self._generate_new_data(records, block_offset))
        self._update_header()

    def scanning_update(self, conditions, attributes):
        # The file header won't change when updating
        total_blk = self._calc(self.rec_tail)[0] + 1
        new_record = convert_str_to_bytes(attributes) + (b'1', -1)
        for block_offset in range(total_blk):
            block = self.buffer_manager.get_file_block(self.filename, block_offset)
            records = self._parse_block_data(block.read(), block_offset)
            for i, record in enumerate(records):
                if self._check_condition(convert_bytes_to_str(record), conditions):
                    records[i] = new_record
            block.write(self._generate_new_data(records, block_offset))

    def _calc(self, record_offset):
        rec_per_blk = BufferManager.block_size // self.record_struct.size
        rec_first_blk = (BufferManager.block_size - self.header_struct.size) // self.record_struct.size
        if record_offset < rec_first_blk:  # in 1st block
            return 0, record_offset
        else:  # not in 1st block
            block_offset = (record_offset - rec_first_blk) // rec_per_blk + 1
            local_offset = record_offset - rec_first_blk - (block_offset - 1) * rec_per_blk
            return block_offset, local_offset

    @staticmethod
    def _check_condition(record, conditions):
        if record[-2] == b'0':  # check the valid bit, return false when meet empty record
            return False
        str_record = convert_bytes_to_str(record[:-2])
        for position, condition in conditions.items():
            value = str_record[position]
            for operator_type, value_restriction in condition.items():
                if operator_type == '=':
                    if value != value_restriction:
                        return False
                elif operator_type == '>':
                    if value <= value_restriction:
                        return False
                elif operator_type == '<':
                    if value >= value_restriction:
                        return False
        return True

    def _generate_new_data(self, records, blk_offset):
        if blk_offset == 0:
            data = bytearray(self.header_struct.size)
        else:
            data = bytearray()
        for r in records:
            data += self.record_struct.pack(*r)
        return data

    def _parse_block_data(self, data, blk_offset):
        upper_bound = len(data)
        if (upper_bound - self.header_struct.size) % self.record_struct.size != 0:
            upper_bound -= self.record_struct.size
        if blk_offset == 0:  # is the first block, need to consider the header
            lower_bound = self.header_struct.size
        else:  # not the first block, all data are records
            lower_bound = 0
        records = [list(self.record_struct.unpack_from(data, offset))
                   for offset in range(lower_bound, upper_bound, self.record_struct.size)]
        return records

    def _parse_header(self):
        # Parse the file header, refresh corresponding info
        # and return the info with a tuple
        block = self.buffer_manager.get_file_block(self.filename, 0)  # Get the first block
        with pin(block):
            data = block.read()
            header_info = self.header_struct.unpack_from(data, 0)
        return header_info

    def _update_header(self):
        # Update the file header after modifying the records
        block = self.buffer_manager.get_file_block(self.filename, 0)
        with pin(block):
            data = block.read()
            header_info = (self.first_free_rec, self.rec_tail)
            data[:self.header_struct.size] = self.header_struct.pack(*header_info)
            block.write(data)


class RecordManager:
    # 1. Need to receive the format of the record for corresponding table. (metadata)
    # 2. Need to receive the table's name. (metadata)
    # 3. Need to receive the record's offset

    header_format = '<ii'  # free_list_head and records_tail.
    header_struct = Struct(header_format)
    file_dir = '/'

    @classmethod
    def init_table(cls, table_name):
        """Initialize the table file"""
        Record.header_format = cls.header_format  # confirm the corresponding info in Record
        Record.header_struct = cls.header_struct
        file_path = cls.file_dir + table_name + '.table'
        if os.path.exists(file_path):
            raise RuntimeError('The file for table \'{}\' has already exists'.format(table_name))
        else:
            with open(file_path, 'w+b') as file:
                file.write(cls.header_struct.pack(*(-1, -1)))

    @classmethod
    def insert(cls, table_name, fmt, attributes):
        """
            insert the given record into a suitable space,
            and return the offset of the inserted record
        """
        file_path = cls.file_dir + table_name + '.table'
        record = Record(file_path, fmt)
        position = record.insert(attributes)
        return position

    @classmethod
    def delete(cls, table_name, fmt, *, with_index, record_offset=None, conditions=None):
        file_path = cls.file_dir + table_name + '.table'
        record = Record(file_path, fmt)
        if with_index:
            if record_offset is None:
                raise RuntimeError('Not specify record offset when using index')
            return record.remove(record_offset)
        else:
            if conditions is None:
                raise RuntimeError('Not specify condition when not using index')
            return record.scanning_delete(conditions)

    @classmethod
    def update(cls, table_name, fmt, attributes, *, with_index, record_offset=None, conditions=None):
        file_path = cls.file_dir + table_name + '.table'
        record = Record(file_path, fmt)
        if with_index:
            if record_offset is None:
                raise RuntimeError('Not specify record offset when using index')
            return record.modify(attributes, record_offset)
        else:
            if conditions is None:
                raise RuntimeError('Not specify condition when not using index')
            return record.scanning_update(conditions, attributes)

    @classmethod
    def select(cls, table_name, fmt, *, with_index, record_offset=None, conditions=None):
        file_path = cls.file_dir + table_name + '.table'
        record = Record(file_path, fmt)
        if with_index:
            if record_offset is None:
                raise RuntimeError('Not specify record offset when using index')
            return record.read(record_offset)
        else:
            if conditions is None:
                raise RuntimeError('Not specify condition when not using index')
            return record.scanning_select(conditions)

    @classmethod
    def set_file_dir(cls, file_dir):
        cls.file_dir = file_dir
