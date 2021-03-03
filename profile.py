# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Hustle Cloudlab Repeatable Experiment Profile

Default behavior:
By default, this uses a c220g5 with 100GB and runs experiments at scale factor 1.
A baseline experiment is always run before numbered experiments.
Numbered experiments will not be run unless provided with one or more flags to use.
Storage size may need to be increased for larger scale factors.

Instructions:
No additional instructions needed. Remember to access experiment results at: /mydata/results
"""

import geni.portal as portal
import geni.rspec.pg as pg

import json

pc = portal.Context()
pc.defineParameter("hardware", "Hardware (Default: c220g5)", portal.ParameterType.STRING, "c220g5")
pc.defineParameter("storage", "Storage Size (Default: 100GB)", portal.ParameterType.STRING, "100GB")
pc.defineParameter("scale_factor", "SSB Scale Factor (Default: 1)", portal.ParameterType.INTEGER, 1)
pc.defineParameter("experiment_1_flags", "Experiment 1 Flags (Default: \"\"", portal.ParameterType.STRING, "")
pc.defineParameter("experiment_2_flags", "Experiment 2 Flags (Default: \"\"", portal.ParameterType.STRING, "")
pc.defineParameter("experiment_3_flags", "Experiment 3 Flags (Default: \"\"", portal.ParameterType.STRING, "")
pc.defineParameter("experiment_4_flags", "Experiment 4 Flags (Default: \"\"", portal.ParameterType.STRING, "")
pc.defineParameter("experiment_5_flags", "Experiment 5 Flags (Default: \"\"", portal.ParameterType.STRING, "")

params = portal.context.bindParameters()

'''
c220g5  224 nodes (Intel Skylake, 20 core, 2 disks)
CPU     Two Intel Xeon Silver 4114 10-core CPUs at 2.20 GHz
RAM     192GB ECC DDR4-2666 Memory
Disk    One 1 TB 7200 RPM 6G SAS HDs
Disk    One Intel DC S3500 480 GB 6G SATA SSD
NIC     Dual-port Intel X520-DA2 10Gb NIC (PCIe v3.0, 8 lanes)
NIC     Onboard Intel i350 1Gb

Note that the sysvol is the SSD, while the nonsysvol is the 7200 RPM HD.
We almost always want to use the sysvol.
'''

rspec = pg.Request()

node = pg.RawPC("node")
node.hardware_type = params.hardware
bs = node.Blockstore("bs", "/mydata")
bs.size = params.storage
bs.placement = "sysvol"

# explicitly copy the needed params for better readability
out_params = {
    "hardware": params.hardware,
    "storage": params.storage,
    "scale_factor": params.scale_factor,
    "experiment_1_flags": params.experiment_1_flags,
    "experiment_2_flags": params.experiment_2_flags,
    "experiment_3_flags": params.experiment_3_flags,
    "experiment_4_flags": params.experiment_4_flags,
    "experiment_5_flags": params.experiment_5_flags,
}

execute_str = \
    "sudo chmod +755 /local/repository/scripts/cloudlab/cloudlab_setup.sh;" + \
    "/local/repository/scripts/cloudlab/cloudlab_setup.sh " + str(params.scale_factor) + ";" + \
    "sudo chmod +755 /mydata/repo/scripts/cloudlab/cloudlab.py" + \
    "/mydata/repo/scripts/cloudlab/cloudlab.py " + json.dumps(out_params) + " >> /mydata/report.txt;"
node.addService(pg.Execute(shell="bash", command=execute_str))

rspec.addResource(node)

pc.printRequestRSpec(rspec)
