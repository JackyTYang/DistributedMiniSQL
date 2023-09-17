import bisect
from struct import Struct
from math import ceil
from collections.abc import Sequence
from buffer_manager import BufferManager, pin


def _convert_to_tuple(element):
    """a helper function to convert element to tuple
    if element is already a sequence, convert it to tuple
    otherwise, create a tuple containing only this element"""
    if isinstance(element, Sequence):
        return tuple(element)
    else:
        return element,


def _convert_to_tuple_list(sequence):
    """a helper function to convert all elements in a sequence to tuple,
    then convert this sequence to a list"""
    return [_convert_to_tuple(x) for x in sequence]


def _encode(element):
    if isinstance(element, str):
        return element.encode('ascii')
    else:
        return element


def _encode_sequence(sequence):
    return tuple(_encode(x) for x in sequence)


def _decode(element):
    if isinstance(element, bytes):
        return element.decode('ascii').rstrip('\0')
    else:
        return element


def _decode_sequence(sequence):
    return tuple(_decode(x) for x in sequence)


def iter_chunk(sequence, offset, chunk_size, total_chunks):
    """iterate a sequence in chunks, beginning from offset"""
    for i in range(total_chunks):
        yield sequence[offset + i * chunk_size: offset + (i + 1) * chunk_size]


class LeafIterator:
    def __init__(self, Node, index_file_path, node, key_position):
        self.Node = Node
        self.index_file_path = index_file_path
        self.node = node
        self.key_position = key_position
        self.manager = BufferManager()

    def __iter__(self):
        return self

    def __next__(self):
        value = self.node.children[self.key_position]
        if self.key_position < len(self.node.keys):
            key = self.node.keys[self.key_position]
            self.key_position += 1
            return key, value
        else:  # jump
            if value == 0:
                raise StopIteration
            else:
                node_block = self.manager.get_file_block(self.index_file_path, value)
                with pin(node_block):
                    self.node = self.Node.frombytes(node_block.read())
                self.key_position = 1
                return self.node.keys[0], self.node.children[0]


def node_factory(fmt):
    """receive a format string, return a Node class"""

    # 首先对Node节点进行了定义Key_struct对键值进行了定义，需要用到辅助函数将所有键值转换为元组类型，然后定义了元数据，由三个
    # int类型数据组成，是否为叶节点，next_deleted状态，子节点位置
    class Node:
        key_struct = Struct(fmt)  # the struct to pack/unpack keys
        meta_struct = Struct('<3i')  # 3 ints: self.next_deleted, self.is_leaf, len(self.keys)
        n = (BufferManager.block_size - 16) // (4 + key_struct.size)

        # n * key_size + 4 * (n + 1) + 4 + 4 + 4 <= block_size
        #                -             -   -   -
        #                ^             ^   ^   ^
        #                |             /   |   \
        #            sizeof(int) is_leaf next len_keys

        def __init__(self, is_leaf, keys, children, next_deleted=0):
            self.is_leaf = is_leaf
            self.keys = _convert_to_tuple_list(keys)
            self.children = list(children)
            self.next_deleted = next_deleted

        def __bytes__(self):
            """the first are self.next_deleted, self.is_leaf, len(self.keys)
            then the keys, then the children, padding to block size with zeroes"""
            key_bytes = b''.join(self.key_struct.pack(*_encode_sequence(x)) for x in self.keys)
            children_struct = Struct('<{}i'.format(len(self.keys) + 1))
            return (self.meta_struct.pack(self.next_deleted, self.is_leaf, len(self.keys))
                    + key_bytes +
                    children_struct.pack(*self.children)).ljust(BufferManager.block_size, b'\0')

        @classmethod
        def frombytes(cls, octets):
            """create a Node object from bytes"""
            next_deleted, is_leaf, len_keys = cls.meta_struct.unpack(octets[:cls.meta_struct.size])
            keys = [_decode_sequence(cls.key_struct.unpack(chunk))
                    for chunk in iter_chunk(octets, cls.meta_struct.size, cls.key_struct.size, len_keys)]
            children_struct = Struct('<{}i'.format(len_keys + 1))
            children_offset = cls.meta_struct.size + len_keys * cls.key_struct.size
            children = list(children_struct.unpack(octets[children_offset:children_offset + children_struct.size]))
            return cls(is_leaf, keys, children, next_deleted)

        # 调用插入，分裂，左移，右移函数对节点内的数据进行更新
        def insert(self, key, value):
            key = _convert_to_tuple(key)
            insert_position = bisect.bisect_left(self.keys, key)
            self.keys.insert(insert_position, key)
            self.children.insert(insert_position, value)

        def split(self, new_block_offset):
            """split into 2 nodes
            if self is leaf node, maintain the link of leaf nodes with the new_block_offset supplied
            otherwise the new_block_offset is not used
            return the new node, the key and value to be inserted into the parent node"""

            split_point = self.n // 2 + 1
            new_node = Node(self.is_leaf,
                            self.keys[split_point:],
                            self.children[split_point:])

            self.keys = self.keys[:split_point]
            self.children = self.children[:split_point]

            if self.is_leaf:
                self.children.append(new_block_offset)  # maintain the leaf link
                return new_node, new_node.keys[0], new_block_offset
            else:
                key = self.keys.pop()  # remove the largest key in the left node
                # this is faster than remove the smallest key in the right
                return new_node, key, new_block_offset

        def fuse_with(self, other, parent, divide_point):
            """fuse the other node into self
            assuming self is on the left, other is on the right"""
            if self.is_leaf and other.is_leaf:
                self.keys.extend(other.keys)
                del self.children[-1]
                self.children.extend(other.children)
                del parent.keys[divide_point]
                del parent.children[divide_point + 1]

            elif not self.is_leaf and not other.is_leaf:
                self.keys.append(parent.keys[divide_point])
                self.keys.extend(other.keys)
                self.children.extend(other.children)
                del parent.keys[divide_point]
                del parent.children[divide_point + 1]

            else:
                raise ValueError('can\'t fuse a leaf node with a non-leaf node')

        def transfer_from_left(self, other, parent, divide_point):
            if other.is_leaf and self.is_leaf:
                self.keys.insert(0, other.keys.pop())
                self.children.insert(0, other.children.pop(-2))
                parent.keys[divide_point] = self.keys[0]
            elif not other.is_leaf and not self.is_leaf:
                self.keys.insert(0, parent.keys[divide_point])
                self.children.insert(0, other.children.pop())
                parent.keys[divide_point] = other.keys.pop()
            else:
                raise ValueError('cannot transfer between leaf nodes and internal nodes')

        def transfer_from_right(self, other, parent, divide_point):
            if self.is_leaf and other.is_leaf:
                self.keys.append(other.keys.pop(0))
                self.children.insert(-1, other.children.pop(0))
                parent.keys[divide_point] = other.keys[0]
            elif not self.is_leaf and not other.is_leaf:
                self.keys.append(parent.keys[divide_point])
                self.children.append(other.children.pop(0))
                parent.keys[divide_point] = other.keys.pop(0)
            else:
                raise ValueError('cannot transfer between leaf nodes and internal nodes')

    return Node


# IndexManager 类定义了 Node，index_file_path,_manager,meta_struct 四个参数，
# 首先向 BufferManager 申请一个 block，然后按照索引位置去找到元数据，还原成索引，如果该位置不存在索引那么就自己创建一个索引。
class IndexManager:
    def __init__(self, index_file_path, fmt):
        """specify the path of the index file and the format of the keys, return a index manager
        if the index file exists, read data from the file
        otherwise create it and initialize its header info
        multiple index manager on the same file MUSTN'T simultaneously exist"""
        self.Node = node_factory(fmt)
        self.index_file_path = index_file_path
        self._manager = BufferManager()
        self.meta_struct = Struct('<4i')  # total blocks, offset of the first deleted block, offset of the root node
        try:
            meta_block = self._manager.get_file_block(self.index_file_path, 0)
            with pin(meta_block):
                self.total_blocks, self.first_deleted_block, self.root, self.first_leaf = self.meta_struct.unpack(
                    meta_block.read()[:self.meta_struct.size])
        except FileNotFoundError:  # create and initialize an index file if not exits
            self.total_blocks, self.first_deleted_block, self.root, self.first_leaf = 1, 0, 0, 0
            with open(index_file_path, 'wb') as f:
                f.write(self.meta_struct.pack(self.total_blocks,
                                              self.first_deleted_block,
                                              self.root,
                                              self.first_leaf).ljust(BufferManager.block_size, b'\0'))

    def dump_header(self):
        """write the header info to the index file
        MUST be called before the program exits,
        otherwise the header info in the file won't be updated"""
        meta_block = self._manager.get_file_block(self.index_file_path, 0)
        with pin(meta_block):
            meta_block.write(self.meta_struct.pack(self.total_blocks,
                                                   self.first_deleted_block,
                                                   self.root,
                                                   self.first_leaf).ljust(BufferManager.block_size, b'\0'))

    def _get_free_block(self):
        """return a free block and update header info, assuming this block will be used"""
        if self.first_deleted_block > 0:
            block_offset = self.first_deleted_block
            block = self._manager.get_file_block(self.index_file_path, block_offset)
            s = Struct('<i')
            next_deleted = s.unpack(block.read()[:s.size])[0]
            self.first_deleted_block = next_deleted
            return block
        else:
            block_offset = self.total_blocks
            block = self._manager.get_file_block(self.index_file_path, block_offset)
            self.total_blocks += 1
            return block

    def _delete_node(self, node, block):
        """delete node and writes it to block
        just a shortcut to mark a block as deleted"""
        with pin(block):
            node.next_deleted = self.first_deleted_block
            block.write(bytes(node))
            self.first_deleted_block = block.block_offset

    def _find_leaf(self, key):
        """find the first leaf node where key may reside
        key may not really reside in this node, in this case, the index file has no such key"""
        key = _convert_to_tuple(key)
        node_block_offset = self.root
        path_to_parents = []
        while True:  # find the insert position
            node_block = self._manager.get_file_block(self.index_file_path, node_block_offset)
            with pin(node_block):
                node = self.Node.frombytes(node_block.read())
                if node.is_leaf:
                    return node, node_block, path_to_parents
                else:  # continue searching
                    path_to_parents.append(node_block_offset)
                    child_index = bisect.bisect_right(node.keys, key)
                    node_block_offset = node.children[child_index]

    def _handle_overflow(self, node, block, path_to_parents):
        if not path_to_parents:  # the root overflowed
            new_block = self._get_free_block()
            new_node, key, value = node.split(new_block.block_offset)
            with pin(block), pin(new_block):
                block.write(bytes(node))
                new_block.write(bytes(new_node))
            new_root_block = self._get_free_block()
            with pin(new_root_block):
                new_root_node = self.Node(False,
                                          [key],
                                          [block.block_offset, new_block.block_offset])
                new_root_block.write(bytes(new_root_node))
            self.root = new_root_block.block_offset
            return
        else:
            parent_offset = path_to_parents.pop()
            new_block = self._get_free_block()
            new_node, key, value = node.split(new_block.block_offset)
            with pin(block), pin(new_block):
                block.write(bytes(node))
                new_block.write(bytes(new_node))
            parent_block = self._manager.get_file_block(self.index_file_path, parent_offset)
            parent_node = self.Node.frombytes(parent_block.read())
            parent_node.insert(key, value)
            if len(parent_node.keys) <= self.Node.n:
                with pin(parent_block):
                    parent_block.write(bytes(parent_node))
            else:
                self._handle_overflow(parent_node, parent_block, path_to_parents)

    def _handle_underflow(self, node, block, path_to_parents):
        """handle underflow after deletion
        will try to transfer from the left sibling first
        then try to transfer from the right sibling
        then try to fuse with the left sibling
        then try to fuse with the right sibling"""
        if block.block_offset == self.root:
            if not node.keys:  # root has no key at all; this node is no longer needed
                if node.is_leaf:
                    self.root = 0
                    self.first_leaf = 0
                else:
                    self.root = node.children[0]
                self._delete_node(node, block)
            else:
                block.write(bytes(node))
            return  # root underflow is not a problem

        parent_offset = path_to_parents.pop()
        parent_block = self._manager.get_file_block(self.index_file_path, parent_offset)
        with pin(parent_block):
            parent = self.Node.frombytes(parent_block.read())
            my_position = bisect.bisect_right(parent.keys, node.keys[0])

        if my_position > 0:  # try find the left sibling
            left_sibling_offset = parent.children[my_position - 1]
            left_sibling_block = self._manager.get_file_block(self.index_file_path,
                                                              left_sibling_offset)
            with pin(left_sibling_block):
                left_sibling = self.Node.frombytes(left_sibling_block.read())
            if len(left_sibling.keys) > ceil(node.n / 2):  # a transfer is possible
                node.transfer_from_left(left_sibling, parent, my_position - 1)
                with pin(block), pin(left_sibling_block), pin(parent_block):
                    block.write(bytes(node))
                left_sibling_block.write(bytes(left_sibling))
                parent_block.write(bytes(parent))
                return
        else:
            left_sibling = None  # no left sibling

        if my_position < len(parent.keys) - 1:  # try find the right sibling
            right_sibling_offset = parent.children[my_position + 1]
            right_sibling_block = self._manager.get_file_block(self.index_file_path,
                                                               right_sibling_offset)
            with pin(right_sibling_block):
                right_sibling = self.Node.frombytes(right_sibling_block.read())
            if len(right_sibling.keys) > ceil(node.n / 2):  # a transfer is possible
                node.transfer_from_right(right_sibling, parent, my_position)
                with pin(block), pin(right_sibling_block), pin(parent_block):
                    block.write(bytes(node))
                    right_sibling_block.write(bytes(right_sibling))
                    parent_block.write(bytes(parent))
                return
        else:
            right_sibling = None  # no right sibling

        if left_sibling is not None:  # fuse with left sibling
            left_sibling.fuse_with(node, parent, my_position - 1)
            with pin(left_sibling_block):
                left_sibling_block.write(bytes(left_sibling))
            self._delete_node(node, block)
            if len(parent.keys) >= ceil(node.n / 2):
                return
            else:
                self._handle_underflow(parent, parent_block, path_to_parents)
        else:  # fuse with right sibling
            node.fuse_with(right_sibling, parent, my_position)
            with pin(block):
                block.write(bytes(node))
            self._delete_node(right_sibling, right_sibling_block)
            if len(parent.keys) >= ceil(node.n / 2):
                return
            else:
                self._handle_underflow(parent, parent_block, path_to_parents)

    def find(self, key):
        """find the smallest key >= (parameter) key
        return an iterator from this position
        raise RuntimeError if the index is empty"""
        key = _convert_to_tuple(key)
        if self.root == 0:
            raise RuntimeError('cannot find from empty index')
        else:
            node, node_block, path_to_parents = self._find_leaf(key)
            key_position = bisect.bisect_left(node.keys, key)
            return LeafIterator(self.Node, self.index_file_path, node, key_position)

    def insert(self, key, value):
        """insert a key-value pair into the index file
        if key already in this index, raise ValueError"""
        key = _convert_to_tuple(key)
        if self.root == 0:
            block = self._get_free_block()
            with pin(block):
                self.root = block.block_offset
                self.first_leaf = self.root
                node = self.Node(is_leaf=True,
                                 keys=[key],
                                 children=[value, 0])
                block.write(bytes(node))
        else:
            node, node_block, path_to_parents = self._find_leaf(key)
            key_position = bisect.bisect_left(node.keys, key)
            if key_position < len(node.keys) and node.keys[key_position] == key:
                raise ValueError('duplicate key {}'.format(key))
            node.insert(key, value)
            if len(node.keys) <= node.n:
                node_block.write(bytes(node))
                return
            else:  # split
                self._handle_overflow(node, node_block, path_to_parents)

    def delete(self, key):
        """delete the key-value pair with key equal the parameter
        if the index file has no such key, raise ValueError"""
        key = _convert_to_tuple(key)
        if self.root == 0:
            raise ValueError('can\'t delete from empty index')
        else:
            node, node_block, path_to_parents = self._find_leaf(key)
            key_position = bisect.bisect_left(node.keys, key)
            if key_position < len(node.keys) and node.keys[key_position] == key:  # key match
                del node.keys[key_position]
                del node.children[key_position]
                if len(node.keys) >= ceil(node.n / 2):
                    node_block.write(bytes(node))
                    return
                else:  # underflow
                    self._handle_underflow(node, node_block, path_to_parents)
            else:  # key doesn't match
                raise ValueError('index has no such key {}'.format(key))

    def iter_leaves(self):
        """return an iterator at the beginning of the leaf node chain"""
        if self.first_leaf == 0:
            raise RuntimeError('can\'t iter from empty index')
        first_leaf_block = self._manager.get_file_block(self.index_file_path, self.first_leaf)
        first_leaf = self.Node.frombytes(first_leaf_block.read())
        return LeafIterator(self.Node, self.index_file_path, first_leaf, 0)
