#!/usr/bin/env bash
#
REPO_ORIG=/local/repository
ATTACHED_STORAGE=/mydata
REPO_ROOT=${ATTACHED_STORAGE}/repo
DEP_ROOT=${ATTACHED_STORAGE}/dep
RESULTS_ROOT=${ATTACHED_STORAGE}/results
SSB_SCRIPT_ROOT=${REPO_ROOT}/scripts/ssb
DEBUG_BUILD=${REPO_ROOT}/build_debug
RELEASE_BUILD=${REPO_ROOT}/build_release
SCRIPT_OUT=${ATTACHED_STORAGE}/report.txt
#
SCALE_FACTOR=$1
#
sudo chmod +777 ${ATTACHED_STORAGE}
mkdir ${DEP_ROOT}
mkdir ${RESULTS_ROOT}
touch ${SCRIPT_OUT}
#
echo "$(date -u) | -=- Setup -=-" &>> ${SCRIPT_OUT}
echo "$(date -u) | Copying repo into larger storage partition..." &>> ${SCRIPT_OUT}
cp -r ${REPO_ORIG} ${REPO_ROOT}
echo "$(date -u) | Installing dependencies..." &>> ${SCRIPT_OUT}
cd ${REPO_ROOT} || exit 10
sudo chmod +755 install_requirements.sh
sudo chmod +755 install_arrow.sh
bash install_requirements.sh
bash install_arrow.sh
echo "$(date -u) | Generating release build..." &>> ${SCRIPT_OUT}
mkdir ${RELEASE_BUILD}
cd ${RELEASE_BUILD} || exit 11
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
echo "$(date -u) | Generating debug build..." &>> ${SCRIPT_OUT}
mkdir ${DEBUG_BUILD}
cd ${DEBUG_BUILD} || exit 12
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j$(nproc)
echo "$(date -u) | Generating SSB data at scale factor: ${SCALE_FACTOR}" &>> ${SCRIPT_OUT}
cd ${SSB_SCRIPT_ROOT} || exit 13
sudo chmod +755 ${SSB_SCRIPT_ROOT}/gen_benchmark_data.sh
bash ${SSB_SCRIPT_ROOT}/gen_benchmark_data.sh "${SCALE_FACTOR}"
echo "$(date -u) | Setup completed." &>> ${SCRIPT_OUT}
