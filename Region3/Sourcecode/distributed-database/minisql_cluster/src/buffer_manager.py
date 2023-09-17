from datetime import datetime
import os
from contextlib import contextmanager


@contextmanager
def pin(block):
    """a context manager for safe pin/unpin
    it is the preferred way to pin a block if the life cycle of the block is known"""
    block.pin()
    yield
    block.unpin()


class Block:
    __slots__ = ['size', '_memory',
                 'file_path', 'block_offset', 'effective_bytes',
                 'dirty', 'pin_count', 'last_accessed_time']

    def __init__(self, size, file_path, block_offset):
        self.size = size
        self._memory = bytearray(size)
        self.file_path = os.path.abspath(file_path)
        self.block_offset = block_offset
        with open(file_path, 'rb') as file:
            file.seek(self.size * block_offset)
            # the remaining data in the file may not be enough to fill the whole block
            # self.effective_bytes store how many bytes are really loaded into memory,
            # which may be updated in future writes
            # it is ultimately used to determine how many bytes are written back to file, in self.flush()
            self.effective_bytes = file.readinto(self._memory)

        self.dirty = False
        self.pin_count = 0

        self.last_accessed_time = datetime.now()

    def read(self):
        """read a block of data from memory"""
        # update last accessed time to support LRU swap algorithm
        self.last_accessed_time = datetime.now()
        return self._memory[:self.effective_bytes]

    def write(self, data, *, trunc=False):
        """write data into memory and adjust self.effective_bytes
        if data size is larger than block size, raise RuntimeError
        unless trunc is asserted to True, when overflowed data will be truncated"""
        data_size = len(data)
        if data_size > self.size:
            if not trunc:
                raise RuntimeError('data size({}B) is larger than block size({}B)'.format(data_size, self.size))
        self.effective_bytes = min(data_size, self.size)
        self._memory[:self.effective_bytes] = data[:self.effective_bytes]
        self.dirty = True
        self.last_accessed_time = datetime.now()

    def flush(self):
        """write data from memory to file"""
        if self.dirty:
            try:
                with open(self.file_path, 'r+b') as file:
                    file.seek(self.block_offset * self.size)
                    file.write(self._memory[:self.effective_bytes])
                    file.flush()
                self.dirty = False
                self.last_accessed_time = datetime.now()
            except FileNotFoundError:
                pass  # suppress this exception
                # based on the assumption that the file won't magically disappear
                # unless the table or index file is deleted from our application
                # in which case flush should just do nothing
                # because it may be a block remained in the buffer manager
                # and this flush is most likely to occur when the buffer manager invokes flush_all()
                # or tries to swap out this block
                # in either case, it will be the end of the life cycle of this block

    def pin(self):
        """pin this block so that it cannot be released"""
        self.pin_count += 1

    def unpin(self):
        """unpin this block so that it can be released"""
        if self.pin_count > 0:
            self.pin_count -= 1
        else:
            raise RuntimeError('this block is already unpinned')


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Singleton(metaclass=SingletonMeta):
    pass


class BufferManager(Singleton):
    block_size = 4096
    total_blocks = 1024

    def __init__(self):
        self._blocks = {}

    def get_file_block(self, file_path, block_offset):
        abs_path = os.path.abspath(file_path)
        if (abs_path, block_offset) in self._blocks:
            # found a cached block
            return self._blocks[(abs_path, block_offset)]
        elif len(self._blocks) < self.total_blocks:
            # has free space
            block = Block(self.block_size, abs_path, block_offset)
            self._blocks[(abs_path, block_offset)] = block
            return block
        else:
            # buffer is full; try to swap out the lru block
            lru_key = None
            lru_block = None
            for key, block in self._blocks.items():
                if block.pin_count == 0:
                    if lru_block is None or block.last_accessed_time < lru_block.last_accessed_time:
                        lru_key = key
                        lru_block = block
            if lru_block is None:
                raise RuntimeError('All blocks are pinned, buffer ran out of blocks')
            else:
                lru_block.flush()
                del self._blocks[lru_key]
                block = Block(self.block_size, abs_path, block_offset)
                self._blocks[(abs_path, block_offset)] = block
                return block

    def detach_from_file(self, file_path):
        """delete all cached blocks associated with the given file"""
        abs_path = os.path.abspath(file_path)
        for key in list(self._blocks):
            if key[0] == abs_path:
                del self._blocks[key]

    def flush_all(self):
        for block in self._blocks.values():
            block.flush()
