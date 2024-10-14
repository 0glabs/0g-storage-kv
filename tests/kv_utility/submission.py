from eth_utils import decode_hex
from math import log2
from utility.submission import split_nodes, create_node, MerkleTree, Leaf
from utility.merkle_tree import add_0x_prefix
from utility.spec import ENTRY_SIZE

def create_submission(data, tags = b""):
    submission = []
    submission.append(len(data))
    submission.append(tags)
    submission.append([])

    offset = 0
    nodes = []
    for chunks in split_nodes(len(data)):
        node_hash = create_node(data, offset, chunks)
        nodes.append(node_hash)

        height = int(log2(chunks))
        submission[2].append([decode_hex(node_hash.decode("utf-8")), height])
        offset += chunks * ENTRY_SIZE

    root_hash = nodes[-1]
    for i in range(len(nodes) - 2, -1, -1):
        tree = MerkleTree()
        tree.add_leaf(Leaf(nodes[i]))
        tree.add_leaf(Leaf(root_hash))
        root_hash = tree.get_root_hash()

    return submission, add_0x_prefix(root_hash.decode("utf-8"))