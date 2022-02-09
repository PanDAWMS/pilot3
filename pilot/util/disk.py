# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Giampaolo Rodola, g.rodola@gmail.com, 2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, Paul.Nilsson@cern.ch, 2021-2022

import os
from collections import namedtuple

ntuple_diskusage = namedtuple('usage', 'total used free')

if hasattr(os, 'statvfs'):  # POSIX
    def disk_usage(path):
        stat = os.statvfs(path)
        free = stat.f_bavail * stat.f_frsize
        total = stat.f_blocks * stat.f_frsize
        used = (stat.f_blocks - stat.f_bfree) * stat.f_frsize
        return ntuple_diskusage(total, used, free)
else:
    def disk_usage(path):
        return ntuple_diskusage(0, 0, 0)

disk_usage.__doc__ = """
Return disk usage statistics about the given path as a (total, used, free)
namedtuple. Values are expressed in bytes.
"""
