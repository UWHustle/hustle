THIRD_PARTY_DIR=`pwd`
if [ "${PWD##*/}" != "third_party" ]; then
  echo "ERROR: This script can be run only from the third party directory"
  exit 1
fi

PATCH_DIR=${THIRD_PARTY_DIR}/patches

third_party_dir_names=("gflags"
"glog"
)

third_party_lib_urls=("https://github.com/gflags/gflags/archive/v2.1.2.tar.gz"
"https://github.com/google/glog/archive/v0.3.5.tar.gz"
)

downloaded_archive_names=("v2.1.2.tar.gz"
"v0.3.5.tar.gz"
)

tar_options=("-xzf"
"-xzf"
)

for ((lib_index=0; lib_index < ${#third_party_dir_names[*]}; lib_index++))
do
  # If the third party directory is not present, create it.
  if [ ! -d ${third_party_dir_names[lib_index]} ]; then
    mkdir ${third_party_dir_names[lib_index]}
  else
    continue
  fi

  # Downaload the compressed archive for the third party library.
  curl_cmd="curl -L -O ${third_party_lib_urls[lib_index]}"
  echo "Downloading from ${third_party_lib_urls[lib_index]} ..."
  echo ${curl_cmd}
  eval ${curl_cmd}
  if [ -f ${downloaded_archive_names[lib_index]} ]; then
    echo "File ${downloaded_archive_names[lib_index]} downloaded successfully"

    # Uncompress the archive to its designated directory.
    # The strip-components option will ensure that all the files directly end up
    # in the desired directory, without any intermediate hierarchy level.
    tar_cmd="tar ${tar_options[lib_index]} ${downloaded_archive_names[lib_index]} -C ${third_party_dir_names[lib_index]} --strip-components=1"
    echo ${tar_cmd}
    echo "Extracting from ${downloaded_archive_names[lib_index]} ..."
    eval ${tar_cmd}

    # Delete the compressed archive.
    rm -rf ${downloaded_archive_names[lib_index]}
  else
    echo "Error downloading file ${downloaded_archive_names[lib_index]} from ${third_party_lib_urls[lib_index]}"
  fi
  # Back to the third_party directory.
  cd ${THIRD_PARTY_DIR}

  if [ "${third_party_dir_names[lib_index]}" == "gflags" ]
  then
    # Apply gflags patch.
    patch ${THIRD_PARTY_DIR}/gflags/CMakeLists.txt ${PATCH_DIR}/gflags/CMakeLists.patch
    patch ${THIRD_PARTY_DIR}/gflags/src/gflags_reporting.cc ${PATCH_DIR}/gflags/gflags_reporting.cc.patch
  fi

  if [ "${third_party_dir_names[lib_index]}" == "glog" ]
  then
    # Apply glog patches.
    patch ${THIRD_PARTY_DIR}/glog/CMakeLists.txt ${PATCH_DIR}/glog/glogCMakeLists.txt.patch
    patch ${THIRD_PARTY_DIR}/glog/src/utilities.cc ${PATCH_DIR}/glog/utilities.cc.patch
  fi
done