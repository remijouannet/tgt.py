#!/usr/bin/env python3

from typing import Optional
from typing import List, Dict, AsyncGenerator
import logging
import asyncio
from datetime import datetime
from pathlib import Path
from asyncio import subprocess
import argparse
import fnmatch
import re
import sys
import pathlib

logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TARGET_REX = re.compile("(?x)((?P<engine>P|L|)@)?(?P<pattern>.+)$")


def list_match(tgt, minion_id=None) -> bool:
    """
    Determines if this host is on the list
    """

    try:
        if (
            ",{},".format(minion_id) in tgt
            or tgt.startswith(minion_id + ",")
            or tgt.endswith("," + minion_id)
        ):
            return True
        return minion_id == tgt
    except (AttributeError, TypeError):
        try:
            return minion_id in tgt
        except Exception:
            return False

    logger.warning(
        "List matcher unexpectedly did not return, for target %s, "
        "this is probably a bug.",
        tgt,
    )
    return False


def match(tgt: str, host: str) -> bool:
    # L: list
    # E: pcre
    # G: glob
    

    results = [] # type: List[str]
    opers = ["and", "or", "not", "(", ")"]

    words = tgt.split()

    while words:
        word = words.pop(0)

        match = TARGET_REX.match(word)
        if not match:
            logger.warning('Unable to parse target "%s"', tgt)
            target_info = {
                "engine": "G",
                "pattern": tgt,
            }
        else:
            target_info = match.groupdict()

        # Easy check first
        if word in opers:
            if results:
                if results[-1] == "(" and word in ("and", "or"):
                    logger.error('Invalid beginning operator after "(": %s', word)
                    return False
                if word == "not":
                    if not results[-1] in ("and", "or", "("):
                        results.append("and")
                results.append(word)
            else:
                # seq start with binary oper, fail
                if word not in ["(", "not"]:
                    logger.error("Invalid beginning operator: %s", word)
                    return False
                results.append(word)

        elif target_info["engine"] == "L":
            results.append(str(list_match(target_info["pattern"], host)))
        elif target_info["engine"] == "P":
            results.append(str(bool(re.match(target_info["pattern"], host))))
        else:
            results.append(str(fnmatch.fnmatch(host, word)))

    results_str = " ".join(results)

    logger.debug('compound_match %s ? "%s" => "%s"', host, tgt, results_str)

    try:
        return eval(results_str)
    except Exception:
        logger.error("Invalid compound target: %s for results: %s", tgt, results_str)
    return False


async def find_host(tgt: str, hostkey_file: pathlib.Path) -> AsyncGenerator[str, None]:
    with open(hostkey_file.expanduser(), "r") as known_hosts:
        for hostkey in known_hosts.readlines():
            # every line can contain hostname1 or hostname1,hostname2,hostname3
            for h in hostkey.strip().split(" ")[0].split(","):
                if match(tgt=tgt, host=h):
                    yield h


async def ssh(
    sem: asyncio.Semaphore, host: str, cmd: str, ssh_options: List, dryrun: bool
) -> None:
    async with sem:
        sshcmd = [
            "ssh",
            *ssh_options,
            host,
            cmd,
        ]
        logger.info("ssh cmd %s: %s", host, sshcmd)

        if dryrun:
            return

        proc = await subprocess.create_subprocess_exec(
            *sshcmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            logger.info("stderr %s\n%s", host, stderr.decode("utf-8"))
        else:
            logger.info("stdout %s\n%s", host, stdout.decode("utf-8"))


async def main() -> None:
    parser = argparse.ArgumentParser(description="host to connect")
    parser.add_argument(
        "--tgt",
        dest="tgt",
        default=None,
        type=str,
        help="target to connect",
    )
    parser.add_argument(
        "--cmd",
        dest="cmd",
        default=None,
        help="cmd",
    )
    parser.add_argument(
        "--parallelism",
        dest="parallelism",
        default=10,
        type=int,
        help="parallelism",
    )
    parser.add_argument(
        "--hostkey",
        dest="hostkey",
        default="~/.ssh/known_hosts",
        type=pathlib.Path,
        help="hostkey",
    )
    parser.add_argument(
        "--dry-run",
        dest="dryrun",
        default=False,
        action="store_true",
        help="hostkey",
    )
    args, ssh_options = parser.parse_known_args()

    logger.info("start")

    tasks = []
    sem = asyncio.Semaphore(args.parallelism)

    async for h in find_host(tgt=args.tgt, hostkey_file=args.hostkey):
        logger.info("found %s", h)
        tasks.append(
            asyncio.create_task(
                ssh(
                    sem=sem,
                    host=h,
                    cmd=args.cmd,
                    ssh_options=ssh_options,
                    dryrun=args.dryrun,
                )
            )
        )

    if tasks:
        await asyncio.wait(tasks)


if __name__ == "__main__":
    asyncio.run(main())
