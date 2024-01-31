import json
import os
import subprocess
import tempfile
import time

from eth_utils import decode_hex
from web3 import Web3, HTTPProvider
from web3.middleware import construct_sign_and_send_raw_middleware
from enum import Enum, unique
from config.node_config import (
    GENESIS_PRIV_KEY,
    GENESIS_PRIV_KEY1,
    TX_PARAMS,
    MINER_ID,
    NO_MERKLE_PROOF_FLAG,
    NO_SEAL_FLAG,
    TX_PARAMS1,
)
from utility.simple_rpc_proxy import SimpleRpcProxy
from utility.utils import (
    initialize_config,
    wait_until,
)


@unique
class BlockChainNodeType(Enum):
    Conflux = 0
    BSC = 1


@unique
class NodeType(Enum):
    BlockChain = 0
    Zgs = 1
    KV = 2


class FailedToStartError(Exception):
    """Raised when a node fails to start correctly."""


class TestNode:
    def __init__(
        self, node_type, index, data_dir, rpc_url, binary, config, log, rpc_timeout=10
    ):
        assert os.path.exists(binary), ("binary not found: %s" % binary)
        self.node_type = node_type
        self.index = index
        self.data_dir = data_dir
        self.rpc_url = rpc_url
        self.config = config
        self.rpc_timeout = rpc_timeout
        self.process = None
        self.stdout = None
        self.stderr = None
        self.config_file = os.path.join(self.data_dir, "config.toml")
        self.args = [binary, "--config", self.config_file]
        self.running = False
        self.rpc_connected = False
        self.rpc = None
        self.log = log

    def __del__(self):
        if self.process:
            self.process.terminate()

    def __getattr__(self, name):
        """Dispatches any unrecognised messages to the RPC connection."""
        assert self.rpc_connected and self.rpc is not None, self._node_msg(
            "Error: no RPC connection"
        )
        return getattr(self.rpc, name)

    def _node_msg(self, msg: str) -> str:
        """Return a modified msg that identifies this node by its index as a debugging aid."""
        return "[node %s %d] %s" % (self.node_type, self.index, msg)

    def _raise_assertion_error(self, msg: str):
        """Raise an AssertionError with msg modified to identify this node."""
        raise AssertionError(self._node_msg(msg))

    def setup_config(self):
        os.mkdir(self.data_dir)
        initialize_config(self.config_file, self.config)

    def start(self, redirect_stderr=False):
        my_env = os.environ.copy()
        if self.stdout is None:
            self.stdout = tempfile.NamedTemporaryFile(
                dir=self.data_dir, prefix="stdout", delete=False
            )
        if self.stderr is None:
            self.stderr = tempfile.NamedTemporaryFile(
                dir=self.data_dir, prefix="stderr", delete=False
            )

        if redirect_stderr:
            self.process = subprocess.Popen(
                self.args,
                stdout=self.stdout,
                stderr=self.stdout,
                cwd=self.data_dir,
                env=my_env,
            )
        else:
            self.process = subprocess.Popen(
                self.args,
                stdout=self.stdout,
                stderr=self.stderr,
                cwd=self.data_dir,
                env=my_env,
            )
        self.running = True

    def wait_for_rpc_connection(self):
        raise NotImplementedError

    def _wait_for_rpc_connection(self, check):
        """Sets up an RPC connection to the node process. Returns False if unable to connect."""
        # Poll at a rate of four times per second
        poll_per_s = 4
        for _ in range(poll_per_s * self.rpc_timeout):
            if self.process.poll() is not None:
                raise FailedToStartError(
                    self._node_msg(
                        "exited with status {} during initialization".format(
                            self.process.returncode
                        )
                    )
                )
            rpc = SimpleRpcProxy(self.rpc_url, timeout=self.rpc_timeout)
            if check(rpc):
                self.rpc_connected = True
                self.rpc = rpc
                return
            time.sleep(1.0 / poll_per_s)
        self._raise_assertion_error(
            "failed to get RPC proxy: index = {}, rpc_url = {}".format(
                self.index, self.rpc_url
            )
        )

    def stop(self, expected_stderr="", kill=False, wait=True):
        """Stop the node."""
        if not self.running:
            return
        if kill:
            self.process.kill()
        else:
            self.process.terminate()
        if wait:
            self.wait_until_stopped()
        # Check that stderr is as expected
        self.stderr.seek(0)
        stderr = self.stderr.read().decode("utf-8").strip()
        # TODO: Check how to avoid `pthread lock: Invalid argument`.
        if stderr != expected_stderr and stderr != "pthread lock: Invalid argument":
            # print process status for debug
            if self.return_code is None:
                self.log.info("Process is still running")
            else:
                self.log.info(
                    "Process has terminated with code {}".format(
                        self.return_code)
                )

            raise AssertionError(
                "Unexpected stderr {} != {} from node={}{}".format(
                    stderr, expected_stderr, self.node_type, self.index
                )
            )

        self.stdout.close()
        self.stderr.close()
        self.stdout = None
        self.stderr = None

    def is_node_stopped(self):
        """Checks whether the node has stopped.

        Returns True if the node has stopped. False otherwise.
        This method is responsible for freeing resources (self.process)."""
        if not self.running:
            return True
        return_code = self.process.poll()
        if return_code is None:
            return False

        # process has stopped. Assert that it didn't return an error code.
        assert return_code == 0, self._node_msg(
            "Node returned non-zero exit code (%d) when stopping" % return_code
        )
        self.running = False
        self.process = None
        self.rpc = None
        self.return_code = return_code
        return True

    def wait_until_stopped(self, timeout=20):
        wait_until(self.is_node_stopped, timeout=timeout)


class BlockchainNode(TestNode):
    def __init__(
        self,
        index,
        data_dir,
        rpc_url,
        binary,
        local_conf,
        contract_path,
        token_contract_path,
        mine_contract_path,
        log,
        blockchain_node_type,
        rpc_timeout=10,
    ):
        self.contract_path = contract_path
        self.token_contract_path = token_contract_path
        self.mine_contract_path = mine_contract_path

        self.blockchain_node_type = blockchain_node_type

        super().__init__(
            NodeType.BlockChain,
            index,
            data_dir,
            rpc_url,
            binary,
            local_conf,
            log,
            rpc_timeout,
        )

    def wait_for_rpc_connection(self):
        self._wait_for_rpc_connection(lambda rpc: rpc.eth_syncing() is False)

    def wait_for_start_mining(self):
        self._wait_for_rpc_connection(
            lambda rpc: int(rpc.eth_blockNumber(), 16) > 0)

    def wait_for_transaction_receipt(self, w3, tx_hash, timeout=120, parent_hash=None):
        return w3.eth.wait_for_transaction_receipt(tx_hash, timeout)

    def setup_contract(self):
        w3 = Web3(HTTPProvider(self.rpc_url))

        account1 = w3.eth.account.from_key(GENESIS_PRIV_KEY)
        account2 = w3.eth.account.from_key(GENESIS_PRIV_KEY1)
        w3.middleware_onion.add(
            construct_sign_and_send_raw_middleware([account1, account2])
        )
        # account = w3.eth.account.from_key(GENESIS_PRIV_KEY1)
        # w3.middleware_onion.add(construct_sign_and_send_raw_middleware(account))

        def deploy_contract(path, args=None):
            if args is None:
                args = []
            contract_interface = json.load(open(path, "r"))
            contract = w3.eth.contract(
                abi=contract_interface["abi"],
                bytecode=contract_interface["bytecode"],
            )
            tx_hash = contract.constructor(*args).transact(TX_PARAMS)
            tx_receipt = self.wait_for_transaction_receipt(w3, tx_hash)
            contract = w3.eth.contract(
                address=tx_receipt.contractAddress,
                abi=contract_interface["abi"],
            )
            return contract, tx_hash

        self.log.debug("Start deploy contracts")
        token_contract, _ = deploy_contract(self.token_contract_path)
        self.log.debug("ERC20 deployed")
        flow_contract, flow_contract_hash = deploy_contract(
            self.contract_path, ["0x0000000000000000000000000000000000000000", 100, 0]
        )
        self.log.debug("Flow deployed")
        mine_contract, _ = deploy_contract(
            self.mine_contract_path,
            [flow_contract.address, "0x0000000000000000000000000000000000000000", 7],
        )
        self.log.debug("Mine deployed")
        self.log.info("All contracts deployed")

        tx_hash = token_contract.functions.approve(
            flow_contract.address, int(1e9)
        ).transact(TX_PARAMS)
        self.wait_for_transaction_receipt(w3, tx_hash)

        # setup second account
        amount = int(1e8)
        tx_hash = token_contract.functions.transfer(account2.address, amount).transact(
            TX_PARAMS
        )
        self.wait_for_transaction_receipt(w3, tx_hash)

        tx_hash = token_contract.functions.approve(
            flow_contract.address, amount
        ).transact(TX_PARAMS1)
        self.wait_for_transaction_receipt(w3, tx_hash)

        tx_hash = mine_contract.functions.setMiner(
            decode_hex(MINER_ID)).transact(TX_PARAMS)
        self.wait_for_transaction_receipt(w3, tx_hash)

        return flow_contract, flow_contract_hash, mine_contract

    def get_contract(self, contract_address):
        w3 = Web3(HTTPProvider(self.rpc_url))

        account1 = w3.eth.account.from_key(GENESIS_PRIV_KEY)
        account2 = w3.eth.account.from_key(GENESIS_PRIV_KEY1)
        w3.middleware_onion.add(
            construct_sign_and_send_raw_middleware([account1, account2])
        )

        contract_interface = json.load(open(self.contract_path, "r"))
        return w3.eth.contract(address=contract_address, abi=contract_interface["abi"])

    def wait_for_transaction(self, tx_hash):
        w3 = Web3(HTTPProvider(self.rpc_url))
        w3.eth.wait_for_transaction_receipt(tx_hash)

    def start(self):
        super().start(self.blockchain_node_type == BlockChainNodeType.BSC)
