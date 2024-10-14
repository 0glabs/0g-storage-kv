#!/usr/bin/env python3

import os

from kv_utility.build_binary import build_zgs
from utility.run_all import run_all

if __name__ == "__main__":
    test_dir = os.path.dirname(__file__)

    tmp_dir = os.path.join(test_dir, "tmp")
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir, exist_ok=True)
    build_zgs(tmp_dir)

    run_all(
        test_dir=test_dir,
        slow_tests={},
        long_manual_tests={},
    )