#!/usr/bin/env python3

"""
# ceph_reweight.py

Incrementally reweight Ceph OSDs in parallel.

Copyright (C) 2017 Canonical Ltd.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License version 3, as published by the Free
Software Foundation.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program.  If not, see <https://www.gnu.org/licenses/>.
"""

__version__ = "0.3.2"
__author__ = "Stephen Mather <stephen.mather@canonical.com>"

import argparse
import json
from subprocess import (
    check_call,
    check_output,
)
from sys import exit
from time import sleep


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Incrementally reweight Ceph OSDs in parallel."
    )
    parser.add_argument(
        "osds", help="specify comma separated names of OSDs to reweight"
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


def check_osds(osds, target):
    """Ensure OSDs exist and are not at target weight."""
    verified_osds = []
    for osd in osds:
        current = current_weight(osd)
        if current is not None:
            if current != target:
                verified_osds.append(osd)
            else:
                print("{} weight is {}, skipping...".format(osd,
                                                            target))
        else:
            print("{} not found, skipping...".format(osd))
    if verified_osds:
        print("Pending: {}".format(", ".join(verified_osds)))
    else:
        print("No OSDs pending.")
    return verified_osds


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
            # Ceph often reweights approximately, so round current weight
            # for basis of comparison with target weight.
            rounded_weight = round(raw_weight, 1)
            print("{} weight: {} ({})".format(osd, raw_weight, rounded_weight))
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


def reweight(verified_osds, target, step):
    """Determine reweight values and perform reweighting."""
    # Store initial list of OSDs to use in summary output.
    osd_list = verified_osds
    # Account for step being given as a negative value.
    step = abs(step)
    while verified_osds:
        if status_ok():
            for osd in verified_osds:
                current = current_weight(osd)
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
            # Give the reweighting a chance to kick off.
            sleep(5)
            verified_osds = check_osds(verified_osds, target)
        else:
            print("Waiting for Ceph status to be suitable for reweighting...")
            sleep(30)
    while not status_ok():
        print("Waiting for Ceph status to be suitable for reweighting...")
        sleep(30)
    print("Reweighted to {}: {}".format(target, ", ".join(osd_list)))
    print("Ceph status is suitable for further reweighting.")


def main():
    """Check given OSDs and reweight if necessary."""
    args = parse_arguments()
    if args.weight < 0:
        exit("Target weight cannot be less than 0.")
    # Ceph often reweights approximately, so round target weight for basis of
    # comparison with current weight.
    target = round(args.weight, 1)
    print("Rounded target weight: {}".format(target))
    verified_osds = check_osds(args.osds.split(","), target)
    if verified_osds:
        reweight(verified_osds, target, args.step)


if __name__ == "__main__":
    main()
