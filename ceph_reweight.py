#!/usr/bin/env python3

"""
# ceph_reweight.py


## Ceph Commands

* `ceph status --format json`
* `ceph health --format json`
* `ceph osd tree --format json`
* `ceph osd crush reweight <osd> <weight>`


## TODO

* Add parallel reweight functionality
* Bump minor when complete
"""

__version__ = "0.1.0"
__author__ = "Stephen Mather <stephen.mather@canonical.com>"

import argparse
import json
from subprocess import (
    check_call,
    check_output,
)
from time import sleep


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Incrementally reweight Ceph OSDs."
    )
    parser.add_argument(
        "osd", help="specify name of OSD to drain"
    )
    parser.add_argument(
        "weight", type=float,
        help=("specify target weight (Ceph often reweights approximately, so "
              "current and target weights will be rounded to one decimal place "
              "for basis of comparison)")
    )
    parser.add_argument(
        "step", type=float, help="specify reweight step"
    )
    args = parser.parse_args()
    return args


def current_weight(osd):
    """Obtain current OSD weight."""
    osd_tree = json.loads(
        check_output(
            ["ceph", "osd", "tree", "--format", "json"],
            universal_newlines=True
        )
    )
    for node in osd_tree["nodes"]:
        if node["name"] == osd:
            raw_weight = node["crush_weight"]
            print("Current raw weight: {}".format(raw_weight))
            # Ceph often reweights approximately, so round current weight
            # for basis of comparison with target weight.
            rounded_weight = round(raw_weight, 1)
            print("Current rounded weight: {}".format(rounded_weight))
            return rounded_weight
    return None


def status_ok():
    """Determine if ceph status is suitable for reweighting."""
    status = json.loads(
        check_output(
            ["ceph", "status", "--format", "json"],
            universal_newlines=True
        )
    )
    status_is_ok = True
    if status["health"]["overall_status"] == "HEALTH_OK":
        return status_is_ok
    for key in ("degraded_ratio",
                "misplaced_ratio",
                "recovering_objects_per_sec"):
        if key in status["pgmap"]:
            value = float(status["pgmap"][key])
            if value:
                status_is_ok = False
                print("{}: {}".format(key, value))
    return status_is_ok


def reweight(osd, current, target, step):
    """Determine reweight values and perform reweight."""
    step = abs(step)
    while current != target:
        if status_ok():
            if target > current:
                next_weight = round(current + step, 2)
                if next_weight > target:
                    next_weight = target
            else:
                next_weight = round(current - step, 2)
                if next_weight < target:
                    next_weight = target
            print("Reweighting {} from {} to {}...".format(osd,
                                                           current,
                                                           next_weight))
            check_call(["ceph", "osd", "crush", "reweight", osd,
                        str(next_weight)])
            sleep(5)  # Give the reweight a chance to kick off.
            current = current_weight(osd)
        else:
            print("Waiting for Ceph status to be suitable for reweighting...")
            sleep(20)  # DEBUG: Revert duration to 60s after testing.
    while not status_ok():
        print("Waiting for Ceph status to be suitable for reweighting...")
        sleep(20)  # DEBUG: Revert duration to 60s after testing.
    print("{} weight is now {}, "
          "Ceph status is suitable for further reweighting."
          .format(osd, current))


def main():
    args = parse_arguments()
    current = current_weight(args.osd)
    # Ceph often reweights approximately, so round target weight for basis of
    # comparison with current weight.
    target = round(args.weight, 1)
    print("Rounded target weight: {}".format(target))
    if current is not None:
        if current != target:
            reweight(args.osd, current, target, args.step)
        else:
            print("{} weight is already {}".format(args.osd, target))
    else:
        print("{} not found".format(args.osd))


if __name__ == "__main__":
    main()
