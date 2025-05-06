#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import argparse
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

# params overwrite priority:
#   for default:
#       default_params < {blackbox,whitebox}_default_params < args
#   for simple:
#       default_params < {blackbox,whitebox}_default_params <
#       simple_default_params <
#       {blackbox,whitebox}_simple_default_params < args
#   for cf_consistency:
#       default_params < {blackbox,whitebox}_default_params <
#       cf_consistency_params < args
#   for txn:
#       default_params < {blackbox,whitebox}_default_params < txn_params < args
#   for ts:
#       default_params < {blackbox,whitebox}_default_params < ts_params < args
#   for multiops_txn:
#       default_params < {blackbox,whitebox}_default_params < multiops_txn_params < args


default_params = {
    "num_threads" : lambda:random.randint(1,4),#default 1
    "data_page_size" : lambda:1 << 12,#default 1<<12
    "data_page_restart_interval" : 16,# lambda:random.choice([8,16,32])
    "index_page_restart_interval" : 16,
    "index_page_read_queue" : lambda:random.randint(500,1000),#default 1024
    "index_buffer_pool_size" : lambda:random.randint(32768,8388608),#default UINT32_MAX
    "init_page_count" : 1 << 15,#default 1<<15
    "pages_per_file_shift" : lambda:11,   #default 11  
    "manifest_limit" : lambda:random.randint(2,16) <<20,#default 8<<20
    "fd_limit" : lambda:random.randint(500,2000),#default 1024
    "io_queue_size" : lambda:random.choice([1024, 2048,4096]),#default 4096
    "buf_ring_size" :lambda:1 << random.choice([8,9,10]),#default 1<<10
    "coroutine_stack_size" :lambda:1024*128,#default 8*1024
}

_TEST_DIR_ENV_VAR = "TEST_TMPDIR"
# If TEST_TMPDIR_EXPECTED is not specified, default value will be TEST_TMPDIR
_TEST_EXPECTED_DIR_ENV_VAR = "TEST_TMPDIR_EXPECTED"
_DEBUG_LEVEL_ENV_VAR = "DEBUG_LEVEL"

stress_cmd = "../build/db_stress/db_stress"
cleanup_cmd = None


def is_release_mode():
    return os.environ.get(_DEBUG_LEVEL_ENV_VAR) == "0"


def get_dbname(test_name):
    test_dir_name = "kvstore_crashtest_" + test_name
    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is None or test_tmpdir == "":
        dbname = tempfile.mkdtemp(prefix=test_dir_name)
    else:
        dbname = test_tmpdir + "/" + test_dir_name
        shutil.rmtree(dbname, True)
        if cleanup_cmd is not None:
            print("Running DB cleanup command - %s\n" % cleanup_cmd)
            # Ignore failure
            os.system(cleanup_cmd)
        try:
            os.mkdir(dbname)
        except OSError:
            pass
    return dbname


expected_values_dir = None


def setup_expected_values_dir():
    global expected_values_dir
    if expected_values_dir is not None:
        return expected_values_dir
    expected_dir_prefix = "rocksdb_crashtest_expected_"
    test_exp_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)

    # set the value to _TEST_DIR_ENV_VAR if _TEST_EXPECTED_DIR_ENV_VAR is not
    # specified.
    if test_exp_tmpdir is None or test_exp_tmpdir == "":
        test_exp_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)

    if test_exp_tmpdir is None or test_exp_tmpdir == "":
        expected_values_dir = tempfile.mkdtemp(prefix=expected_dir_prefix)
    else:
        # if tmpdir is specified, store the expected_values_dir under that dir
        expected_values_dir = test_exp_tmpdir + "/rocksdb_crashtest_expected"
        if os.path.exists(expected_values_dir):
            shutil.rmtree(expected_values_dir)
        os.mkdir(expected_values_dir)
    return expected_values_dir


multiops_txn_key_spaces_file = None


def setup_multiops_txn_key_spaces_file():
    global multiops_txn_key_spaces_file
    if multiops_txn_key_spaces_file is not None:
        return multiops_txn_key_spaces_file
    key_spaces_file_prefix = "rocksdb_crashtest_multiops_txn_key_spaces"
    test_exp_tmpdir = os.environ.get(_TEST_EXPECTED_DIR_ENV_VAR)

    # set the value to _TEST_DIR_ENV_VAR if _TEST_EXPECTED_DIR_ENV_VAR is not
    # specified.
    if test_exp_tmpdir is None or test_exp_tmpdir == "":
        test_exp_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)

    if test_exp_tmpdir is None or test_exp_tmpdir == "":
        multiops_txn_key_spaces_file = tempfile.mkstemp(prefix=key_spaces_file_prefix)[
            1
        ]
    else:
        if not os.path.exists(test_exp_tmpdir):
            os.mkdir(test_exp_tmpdir)
        multiops_txn_key_spaces_file = tempfile.mkstemp(
            prefix=key_spaces_file_prefix, dir=test_exp_tmpdir
        )[1]
    return multiops_txn_key_spaces_file


def is_direct_io_supported(dbname):
    with tempfile.NamedTemporaryFile(dir=dbname) as f:
        try:
            os.open(f.name, os.O_DIRECT)
        except BaseException:
            return False
        return True


blackbox_default_params = {
    "duration": 6000,
    "interval": 60,
}

whitebox_default_params={
    "duration":6000,
    "kill_odds":100000,
}



def finalize_and_sanitize(src_params):
    dest_params = {k: v() if callable(v) else v for (k, v) in src_params.items()}
    return dest_params


def gen_cmd_params(args):
    params = {}

    params.update(default_params)
    if args.test_type == "blackbox":
        params.update(blackbox_default_params)
    if args.test_type == "whitebox":
        params.update(whitebox_default_params)

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v
    return params


def gen_cmd(params, unknown_params):
    finalzied_params = finalize_and_sanitize(params)
    cmd = (
        [stress_cmd]
        + [
            f"--{k}={v}"
            for k, v in [(k, finalzied_params[k]) for k in sorted(finalzied_params)]
            if k
            not in {
                "test_type",
                "simple",
                "duration",
                "interval",
                "cf_consistency",
                "txn",
                "optimistic_txn",
                "test_best_efforts_recovery",
                "enable_ts",
                "test_multiops_txn",
                "write_policy",
                "stress_cmd",
                "test_tiered_storage",
                "cleanup_cmd",
                "skip_tmpdir_check",
                "print_stderr_separately",
                "verify_timeout",
            }
            and v is not None
        ]
        + unknown_params
    )
    return cmd


def execute_cmd(cmd, timeout=None, timeout_pstack=False):
    child = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    print("Running db_stress with pid=%d: %s\n" % (child.pid, " ".join(cmd)))
    print("This process will be killed manually after interval=",timeout)
    pid = child.pid

    try:
        outs, errs = child.communicate(timeout=timeout)
        hit_timeout = False
        print("WARNING: db_stress ended before kill: exitcode=%d\n" % child.returncode)
    except subprocess.TimeoutExpired:
        hit_timeout = True
        if timeout_pstack:
            os.system("pstack %d" % pid)
        child.kill()
        print("\nkilled process manually, pid=%d\n" % child.pid)
        outs, errs = child.communicate()

    return hit_timeout, child.returncode, outs.decode("utf-8"), errs.decode("utf-8")


def print_output_and_exit_on_error(stdout, stderr, print_stderr_separately=False):
    if len(stdout)>0:
        print("stdout:\n", stdout)
    else:
        print("stdout:None")
    if len(stderr) == 0:
        print("stderr:None\n")
        return

    if print_stderr_separately:
        print("stderr:\n", stderr, file=sys.stderr)
    else:
        print("stderr:\n", stderr)


def cleanup_after_success(dbname):
    shutil.rmtree(dbname, True)
    if cleanup_cmd is not None:
        print("Running DB cleanup command - %s\n" % cleanup_cmd)
        ret = os.system(cleanup_cmd)
        if ret != 0:
            print("WARNING: DB cleanup returned error %d\n" % ret)


# This script runs and kills db_stress multiple times. It checks consistency
# in case of unsafe crashes in RocksDB.
def blackbox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname("blackbox")
    exit_time = time.time() + cmd_params["duration"]

    print(
        "Running blackbox-crash-test with \n"
        + "total-duration="
        + str(cmd_params["duration"])
        + "\n"
    )

    while time.time() < exit_time:
        cmd = gen_cmd(
            dict(list(cmd_params.items())), unknown_args
        )

        interval=random.randint(50,70)
        hit_timeout, retcode, outs, errs = execute_cmd(cmd, interval)

        if not hit_timeout:
            print("Exit Before Killing")
            print_output_and_exit_on_error(outs, errs, args.print_stderr_separately)
            sys.exit(2)

        print_output_and_exit_on_error(outs, errs, args.print_stderr_separately)
        print("\n\n\n")

        time.sleep(1)  # time to stabilize before the next run

        time.sleep(1)  # time to stabilize before the next run
    print("blackbox crash test has succeeded!!!")
    # we need to clean up after ourselves -- only do this on test success
    cleanup_after_success(dbname)


def whitebox_crash_main(args, unknown_args):
    cmd_params = gen_cmd_params(args)
    dbname = get_dbname("whitebox")

    cur_time = time.time()
    exit_time = cur_time + cmd_params["duration"]

    print(
        "Running whitebox-crash-test with \n"
        + "total-duration="
        + str(cmd_params["duration"])
        + "\n"
    )
    odd=cmd_params["kill_odds"]
    succeeded = True
    hit_timeout = False
    while time.time() < exit_time:
        cmd = gen_cmd(
            dict(
                list(cmd_params.items())
            ),
            unknown_args,
        )

        hit_timeout, retncode, stdoutdata, stderrdata = execute_cmd(
            cmd, exit_time - time.time() + 900
        )
        msg = " kill_odds=1/{}, exitcode={}\n".format(
                 odd,   retncode
        )

        print(msg)
        print_output_and_exit_on_error(
            stdoutdata, stderrdata, args.print_stderr_separately
        )

        if hit_timeout:
            print("Killing the run for running too long")
            break

        succeeded = False
        if odd>0 and (retncode == 100):
            succeeded = True
            print("killed process at KillPoint successfully\n\n\n\n")

        if not succeeded:
            print("TEST FAILED. See kill option and exit code above!!!\n")
            sys.exit(1)

        time.sleep(1)  # time to stabilize after a kill

    print("whitebox crash test has succeeded!!!")

    # Clean up after ourselves
    if succeeded or hit_timeout:
        cleanup_after_success(dbname)


def main():
    global stress_cmd
    global cleanup_cmd

    parser = argparse.ArgumentParser(
        description="This script runs and kills \
        db_stress multiple times"
    )
    parser.add_argument("test_type", choices=["blackbox", "whitebox"])
    parser.add_argument("--simple", action="store_true")
    parser.add_argument("--cf_consistency", action="store_true")
    parser.add_argument("--txn", action="store_true")
    parser.add_argument("--optimistic_txn", action="store_true")
    parser.add_argument("--test_best_efforts_recovery", action="store_true")
    parser.add_argument("--enable_ts", action="store_true")
    parser.add_argument("--test_multiops_txn", action="store_true")
    parser.add_argument("--write_policy", choices=["write_committed", "write_prepared"])
    parser.add_argument("--stress_cmd")
    parser.add_argument("--test_tiered_storage", action="store_true")
    parser.add_argument("--cleanup_cmd")
    parser.add_argument("--skip_tmpdir_check", action="store_true")
    parser.add_argument("--print_stderr_separately", action="store_true", default=False)

    all_params = dict(
        list(default_params.items())
        + list(blackbox_default_params.items())
        +list(whitebox_default_params.items())
    )

    for k, v in all_params.items():
        parser.add_argument("--" + k, type=type(v() if callable(v) else v))
    # unknown_args are passed directly to db_stress
    args, unknown_args = parser.parse_known_args()

    test_tmpdir = os.environ.get(_TEST_DIR_ENV_VAR)
    if test_tmpdir is not None and not args.skip_tmpdir_check:
        isdir = False
        try:
            isdir = os.path.isdir(test_tmpdir)
            if not isdir:
                print(
                    "ERROR: %s env var is set to a non-existent directory: %s. Update it to correct directory path."
                    % (_TEST_DIR_ENV_VAR, test_tmpdir)
                )
                sys.exit(1)
        except OSError:
            pass

    if args.stress_cmd:
        stress_cmd = args.stress_cmd
    if args.cleanup_cmd:
        cleanup_cmd = args.cleanup_cmd
    if args.test_type == "blackbox":
        blackbox_crash_main(args, unknown_args)
    if args.test_type=="whitebox":
        whitebox_crash_main(args,unknown_args)


if __name__ == "__main__":
    main()