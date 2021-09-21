#!/usr/bin/env python

import argparse
import logging
import os
import requests
import subprocess
import sys
import tempfile
from typing import List

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stderr)
formatter = logging.Formatter(
    fmt="[%(levelname)s %(asctime)s] " "%(filename)s: %(lineno)d  " "%(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def wheel_url(ray_version, git_branch, git_commit):
    return (
        f"https://s3-us-west-2.amazonaws.com/ray-wheels/"
        f"{git_branch}/{git_commit}/"
        f"ray-{ray_version}-cp37-cp37m-manylinux2014_x86_64.whl"
    )


def wheel_exists(ray_version, git_branch, git_commit):
    url = wheel_url(ray_version, git_branch, git_commit)
    return requests.head(url).status_code == 200


def get_latest_commits(repo: str, branch: str) -> List[str]:
    cur = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)

        clone_cmd = [
            "git",
            "clone",
            "--filter=tree:0",
            "--no-checkout",
            # "--single-branch",
            # "--depth=10",
            f"--branch={branch}",
            repo,
            tmpdir,
        ]
        log_cmd = [
            "git",
            "log",
            "-n",
            "10",
            "--pretty=format:%H",
        ]

        logger.info(f"clone: {clone_cmd}")
        subprocess.check_output(clone_cmd)
        logger.info(f"get commits: {log_cmd}")
        commits = (
            subprocess.check_output(log_cmd).decode(sys.stdout.encoding).split("\n")
        )
    os.chdir(cur)
    return commits


def find_ray_wheels(repo: str, branch: str, version: str):
    url = None
    commits = get_latest_commits(repo, branch)
    logger.info(f"Latest 10 commits for branch {branch}: {commits}")
    for commit in commits:
        if wheel_exists(version, branch, commit):
            url = wheel_url(version, branch, commit)
            os.environ["RAY_WHEELS"] = url
            os.environ["RAY_COMMIT"] = commit
            logger.info(
                f"Found wheels URL for Ray {version}, branch {branch}: " f"{url}"
            )
            break
    return url


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("branch", type=str, help="git branch")
    parser.add_argument("--version", type=str, default="2.0.0.dev0", help="ray version")
    parser.add_argument(
        "--repo",
        type=str,
        default="https://github.com/ray-project/ray.git",
        help="ray repo",
    )
    args, _ = parser.parse_known_args()
    wheels = find_ray_wheels(args.repo, args.branch, args.version)
    if wheels is not None:
        print(wheels)
    sys.exit(2)
