#!/usr/bin/env python3
import base64
from this import d
import time
from os import access
import random
from test_framework.test_framework import TestFramework
from utility.kv import (
    MAX_U64,
    op_with_address,
    op_with_key,
    STREAM_DOMAIN,
    with_prefix,
    is_write_permission_denied,
    MAX_STREAM_ID,
    pad,
    to_key_with_size,
    to_stream_id,
    create_kv_data,
    AccessControlOps,
    rand_key,
    rand_write,
)
from utility.submission import submit_data
from utility.submission import create_submission
from utility.utils import (
    assert_equal,
    wait_until,
)
from config.node_config import TX_PARAMS, TX_PARAMS1, GENESIS_ACCOUNT, GENESIS_ACCOUNT1


class KVPutGetTest(TestFramework):
    def setup_params(self):
        self.num_blockchain_nodes = 1
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
    ):
        chunk_data, tags = create_kv_data(version, reads, writes, access_controls)
        if trunc:
            chunk_data = chunk_data[
                : random.randrange(len(chunk_data) // 2, len(chunk_data))
            ]
        submissions, data_root = create_submission(
            chunk_data, tags if given_tags is None else given_tags
        )
        self.contract.submit(submissions, tx_params=tx_params)
        wait_until(lambda: self.contract.num_submissions() == self.next_tx_seq + 1)

        client = self.nodes[0]
        wait_until(lambda: client.zgs_get_file_info(data_root) is not None)

        segments = submit_data(client, chunk_data)
        wait_until(lambda: client.zgs_get_file_info(data_root)["finalized"])

    def update_data(self, writes):
        for write in writes:
            self.data[",".join([write[0], write[1]])] = (
                write[3] if len(write[3]) > 0 else None
            )

    def write_streams(self):
        # first put
        writes = [rand_write() for i in range(20)]
        self.submit(MAX_U64, [], writes, [])
        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "Commit"
        )
        first_version = self.next_tx_seq
        self.next_tx_seq += 1

        # check data and admin role
        self.update_data(writes)
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value)
            assert_equal(
                self.kv_nodes[0].kv_is_admin(GENESIS_ACCOUNT.address, stream_id), True
            )

        # overwrite
        writes = []
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            writes.append(rand_write(stream_id, key))
        self.submit(first_version, [], writes, [])
        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "Commit"
        )
        second_version = self.next_tx_seq
        self.next_tx_seq += 1
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value, first_version)
        self.update_data(writes)
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value, second_version)

        # write but conflict
        writes = []
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            writes.append(rand_write(stream_id, key))
        self.submit(first_version, [], writes, [])
        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "VersionConfliction"
        )
        self.next_tx_seq += 1

        writes = writes[:1]
        reads = []
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            reads.append([stream_id, key])
        self.submit(first_version, reads, writes, [])
        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "VersionConfliction"
        )
        self.next_tx_seq += 1

        # write but invalid format
        writes = []
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            writes.append(rand_write(stream_id, key))
        self.submit(MAX_U64, [], writes, [], trunc=True)
        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "DataParseError: Invalid stream data"
        )
        self.next_tx_seq += 1

        # write but permission denied
        self.submit(MAX_U64, [], writes, [], tx_params=TX_PARAMS1)
        wait_until(
            lambda: is_write_permission_denied(
                self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            )
        )
        self.next_tx_seq += 1

        # check data
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value)

        # overwrite, write same key multiple times in one tx
        writes = []
        reads = []
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            for _ in range(random.randrange(3, 10)):
                writes.append(rand_write(stream_id, key))
            reads.append([stream_id, key])
        random.shuffle(writes)
        self.submit(second_version, [], writes, [])
        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "Commit"
        )
        third_version = self.next_tx_seq
        self.next_tx_seq += 1
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value, third_version - 1)
        self.update_data(writes)
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value)

        # delete
        writes = []
        reads = []
        flag = 0
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            if flag % 2 == 0:
                writes.append(rand_write(stream_id, key, 0))
        self.submit(MAX_U64, [], writes, [])
        wait_until(
            lambda: self.kv_nodes[0].kv_get_trasanction_result(self.next_tx_seq)
            == "Commit"
        )
        fourth_version = self.next_tx_seq
        self.next_tx_seq += 1
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value, fourth_version - 1)
        self.update_data(writes)
        for stream_id_key, value in self.data.items():
            stream_id, key = stream_id_key.split(",")
            self.kv_nodes[0].check_equal(stream_id, key, value)


if __name__ == "__main__":
    KVPutGetTest().main()
