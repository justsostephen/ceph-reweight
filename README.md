# ceph_reweight.py

Incrementally reweight Ceph OSDs in parallel.

Copyright (C) 2017 Canonical Ltd.

This file is licensed under the Creative Commons Attribution-ShareAlike 4.0 International Public License (CC BY-SA 4.0).  To view a copy of the license, visit "https://creativecommons.org/licenses/by-sa/4.0/legalcode".


## Version History


### 0.1.0

* Initial release


### 0.2.0

* Target weight rounded for basis of comparison with current weight
* Removed `sudo` from commands
* os functions substituted for subprocess functions
* Switched from plain to JSON Ceph output
* Ceph status now determined based on pgmap degraded_ratio, misplaced_ratio and
  recovering_objects_per_sec, in addition to overall health status


### 0.3.0

* Added parallel reweighting functionality


### 0.3.1

* Added README


### 0.3.2

* Added copyright and license details
