#!/usr/bin/env python3

"""
# ceph_reweight.py
"""

__version__ = "0.1.0"
__author__ = "Stephen Mather <stephen.mather@canonical.com>"

import argparse
from os import popen, system
from time import sleep


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("osd", help="specify name of OSD to drain")
    parser.add_argument("weight", type=float, help="specify target weight")
    parser.add_argument("step", type=float, help="specify reweight step")
    args = parser.parse_args()
    return args


def current_weight(osd):
    """Obtain current node weight."""
    with popen("sudo ceph osd tree") as osd_tree:
        for line in osd_tree:
            if osd + " " in line:
                raw_weight = line.split()[1]
                print("Raw weight: {}".format(raw_weight))
                weight = round(float(raw_weight), 1)
                print("Rounded weight: {}".format(weight))
                return weight
        return None


def ceph_health():
    """Obtain ceph health status."""
    with popen("sudo ceph health") as health:
        status = health.read().split()[0]
        print("Ceph status: {}".format(status))
    return status


def reweight(osd, current, target, step):
    """Determine reweight values and perform reweight."""
    step = abs(step)
    while current != target:
        if ceph_health() == "HEALTH_OK":
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
            system("sudo ceph osd crush reweight {} {}".format(osd,
                                                               next_weight))
            sleep(5)  # Give the reweight a chance to kick off.
            current = current_weight(osd)
        else:
            print('Waiting for `ceph health` to return "HEALTH_OK"...')
            sleep(60)
    while ceph_health() != "HEALTH_OK":
        print('Waiting for `ceph health` to return "HEALTH_OK"...')
        sleep(60)
    print("{} weight is now {}, ceph health is OK.".format(osd, current))


def main():
    args = parse_arguments()
    current = current_weight(args.osd)
    if current is not None:
        if current != args.weight:
            reweight(args.osd, current, args.weight, args.step)
        else:
            print("{} weight is already {}".format(args.osd, args.weight))
    else:
        print("{} not found".format(args.osd))


if __name__ == "__main__":
    main()
