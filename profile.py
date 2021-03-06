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
By default, this uses a c220g5 with 100GB of storage and runs experiments at scale factor 1.
Numbered experiments will not be run unless provided with one or more arguments to use.
A common argument can be provided that will precede all per-experiment arguments.
Storage size may need to be increased for larger scale factors.

Instructions:
No additional instructions needed. Remember to access experiment results at: /mydata/results
"""

import geni.portal as portal
import geni.rspec.pg as pg

import json

try:
    import urllib.parse as url_parser
except ImportError:
    import urllib as url_parser

pc = portal.Context()
pc.defineParameter("hardware", "Hardware (Default: c220g5)", portal.ParameterType.STRING, "c220g5")
pc.defineParameter("storage", "Storage Size (Default: 100GB)", portal.ParameterType.STRING, "100GB")
pc.defineParameter("scale_factor", "SSB Scale Factor (Default: 1)", portal.ParameterType.INTEGER, 1)
pc.defineParameter("common_args",
                   "Common Experiment Args (Default: \"ssb hash-aggregate\", replace with \"skip\" if not in use.)",
                   portal.ParameterType.STRING, "ssb hash-aggregate")
pc.defineParameter("experiment_1_args", "Experiment 1 Args (Default: \"skip\")", portal.ParameterType.STRING, "skip")
pc.defineParameter("experiment_2_args", "Experiment 2 Args (Default: \"skip\")", portal.ParameterType.STRING, "skip")
pc.defineParameter("experiment_3_args", "Experiment 3 Args (Default: \"skip\")", portal.ParameterType.STRING, "skip")
pc.defineParameter("experiment_4_args", "Experiment 4 Args (Default: \"skip\")", portal.ParameterType.STRING, "skip")
pc.defineParameter("experiment_5_args", "Experiment 5 Args (Default: \"skip\")", portal.ParameterType.STRING, "skip")

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
    "common_args": params.common_args,
    "experiment_1_args": params.experiment_1_args,
    "experiment_2_args": params.experiment_2_args,
    "experiment_3_args": params.experiment_3_args,
    "experiment_4_args": params.experiment_4_args,
    "experiment_5_args": params.experiment_5_args,
}
enc_str = url_parser.quote_plus((json.dumps(out_params, separators=(',', ':'))))

execute_str = \
    "sudo touch /mydata/params.json;" + \
    "sudo chmod +777 /mydata/params.json;" + \
    "echo " + enc_str + " > /mydata/params.json;" + \
    "sudo chmod +777 /local/repository/scripts/cloudlab/cloudlab_setup.sh;" + \
    "/local/repository/scripts/cloudlab/cloudlab_setup.sh " + str(params.scale_factor) + ";" + \
    "sudo chmod +777 /mydata/repo/scripts/cloudlab/cloudlab.py;" + \
    "python3 /mydata/repo/scripts/cloudlab/cloudlab.py >> /mydata/report.txt 2>&1;"
node.addService(pg.Execute(shell="bash", command=execute_str))

rspec.addResource(node)

pc.printRequestRSpec(rspec)
