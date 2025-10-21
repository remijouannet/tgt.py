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
import ast

logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TARGET_RE = re.compile("(?x)((?P<engine>P|L|)@)?(?P<pattern>.+)$")

class bcolors:
    GREEN = '\033[0;32m'
    RED = '\033[0;31m'
    ENDC = '\033[0m'


def match(tgt: str, host: str) -> bool:
    # L: list
    # E: pcre
    # G: glob

    results = []  # type: List[str]
    opers = ["and", "or", "not", "(", ")"]

    words = tgt.split(" ")

    while words:
        word = words.pop(0)

        match = TARGET_RE.match(word)
        if not match:
            logger.warning('Unable to parse target "%s"', tgt)
            target_info = {
                "engine": "G",
                "pattern": tgt,
            }
        else:
            target_info = match.groupdict()

        if word in opers:
            results.append(word)
        elif target_info["engine"] == "L":
            results.append(str(host in target_info["pattern"].split(",")))
        elif target_info["engine"] == "P":
            results.append(str(bool(re.match(target_info["pattern"], host))))
        else:
            results.append(str(fnmatch.fnmatch(host, word)))

    result_parse = ast.parse(" ".join(results), filename="", mode="eval")

    logger.debug('compound_match %s ? "%s" => "%s"', host, tgt, ast.dump(result_parse))

    if isinstance(result_parse.body, ast.BoolOp):
        return eval(compile(result_parse, filename="", mode="eval"))
    elif isinstance(result_parse.body, ast.Constant):
        if isinstance(result_parse.body.value, bool):
            return eval(compile(result_parse, filename="", mode="eval"))
    else:
        return False

async def find_host(tgt: str, list: str, hostkey_file: pathlib.Path) -> AsyncGenerator[str, None]:
    if list:
        with open(list, "r") as f:
            for h in f.readlines():
                yield h.strip()
    elif tgt:
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
            logger.info("%s stderr %s\n%s %s", bcolors.RED, host, stderr.decode("utf-8"), bcolors.ENDC)
        else:
            logger.info("%s stdout %s\n%s %s", bcolors.GREEN, host, stdout.decode("utf-8"), bcolors.ENDC)

def get_main_parser():
    parser = argparse.ArgumentParser(description="host to connect")
    parser.add_argument(
        "--tgt",
        dest="tgt",
        default=None,
        type=str,
        help="target to connect",
    )
    parser.add_argument(
        "--list",
        dest="list",
        default=None,
        type=str,
        help="list of target to connect",
    )
    parser.add_argument(
        "--cmd",
        dest="cmd",
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
        help="dry-run",
    )
    return parser

async def main() -> None:
    args, ssh_options = get_main_parser().parse_known_args()

    logger.info("start")

    tasks = []
    hosts = []
    sem = asyncio.Semaphore(args.parallelism)

    async for h in find_host(tgt=args.tgt, list=args.list, hostkey_file=args.hostkey):
        if h in hosts:
            #skip duplicate
            continue
        else:
            hosts.append(h)

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
