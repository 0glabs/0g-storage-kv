#!/usr/bin/env python3
import random
from kv_test_framework.test_framework import KVTestFramework
from test_framework.conflux_node import connect_nodes, disconnect_nodes, sync_blocks
from test_framework.blockchain_node import BlockChainNodeType
from utility.kv import (
    MAX_U64,
    MAX_STREAM_ID,
    to_stream_id,
    create_kv_data,
    rand_write,
)
from utility.submission import submit_data
from kv_utility.submission import create_submission
from utility.utils import (
    assert_equal,
    wait_until,
)
from config.node_config import TX_PARAMS, GENESIS_ACCOUNT


class ReorgTest(KVTestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 2
        self.num_nodes = 1

    def run_test(self):
        # setup kv node, watch stream with id [0,100)
        self.stream_ids = [to_stream_id(i) for i in range(MAX_STREAM_ID)]
        self.stream_ids.reverse()

        self.setup_kv_node(0, self.stream_ids)
        self.stream_ids.reverse()
        assert_equal(
            [x[2:] for x in self.kv_nodes[0].kv_get_holding_stream_ids()],
            self.stream_ids,
        )

        # tx_seq and data mapping
        self.next_tx_seq = 0
        self.data = {}
        # write empty stream
        self.write_streams()

    def submit(
        self,
        version,
        reads,
        writes,
        access_controls,
        tx_params=TX_PARAMS,
        given_tags=None,
        trunc=False,
        node_index=0,
    ):
        chunk_data, tags = create_kv_data(version, reads, writes, access_controls)
        if trunc:
            chunk_data = chunk_data[
                : random.randrange(len(chunk_data) // 2, len(chunk_data))
            ]
        submissions, data_root = create_submission(
            chunk_data, tags if given_tags is None else given_tags
        )
        self.contract.submit(submissions, tx_prarams=tx_params, node_idx=node_index)
        wait_until(
            lambda: self.contract.num_submissions(node_index) == self.next_tx_seq + 1
        )

        return data_root, chunk_data

    def submit_data(self, data_root, chunk_data):
        client = self.nodes[0]
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segments = submit_data(client, chunk_data)
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

    def update_data(self, writes):
        for write in writes:
            self.data[",".join([write[0], write[1]])] = write[3]

    def write_streams(self):
        disconnect_nodes(self.blockchain_nodes, 0, 1)
        self.blockchain_nodes[0].generate_empty_blocks(5)

        # first put
        writes = [rand_write() for i in range(20)]
        data_root, chunk_data = self.submit(MAX_U64, [], writes, [])
        client = self.nodes[0]
        assert client.zgs_get_file_info(data_root) is None
        self.blockchain_nodes[0].generate_empty_blocks(12)
        self.submit_data(data_root, chunk_data)
        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "Commit",
        )

        # check data and admin role
        self.update_data(writes)
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value)
            assert_equal(
                self.kv_nodes[0].kv_is_admin(GENESIS_ACCOUNT.address, stream_id), True
            )

        # reorg put
        writes = [rand_write() for i in range(20)]
        data_root, chunk_data = self.submit(MAX_U64, [], writes, [], node_index=1)

        self.blockchain_nodes[1].generate_empty_blocks(30)
        connect_nodes(self.blockchain_nodes, 0, 1)
        sync_blocks(self.blockchain_nodes[0:2])

        self.submit_data(data_root, chunk_data)

        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "Commit",
        )

        self.data = {}
        self.update_data(writes)
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value)


if __name__ == "__main__":
    ReorgTest(blockchain_node_type=BlockChainNodeType.Conflux).main()
