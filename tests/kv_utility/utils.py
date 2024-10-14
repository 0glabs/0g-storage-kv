from utility.utils import PortMin, MAX_NODES

def kv_rpc_port(n):
    return PortMin.n + 5 * MAX_NODES + n
