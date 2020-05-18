THIRD_PARTY_DIR=`pwd`
if["${PWD##*/}"!="third_party"];then
echo"ERROR:Thisscriptcanberunonlyfromthethirdpartydirectory"
exit1
fi

PATCH_DIR=${THIRD_PARTY_DIR}/patches

cdthird_party

third_party_dir_names=("gflags"
"glog"
)

third_party_lib_urls=("https://github.com/gflags/gflags/archive/v2.1.2.tar.gz"
"https://github.com/google/glog/archive/v0.3.5.tar.gz"
)

downloaded_archive_names=("v2.1.2.tar.gz"
"v3.4.5.tar.gz"
)

tar_options=("-xzf"
"-xzf"
)

for((lib_index=0;lib_index<${#third_party_dir_names[*]};lib_index++))
do
#Ifthethirdpartydirectoryisnotpresent,createit.
if[!-d${third_party_dir_names[lib_index]}];then
mkdir${third_party_dir_names[lib_index]}
fi

#Downaloadthecompressedarchiveforthethirdpartylibrary.
curl_cmd="curl-L-O${third_party_lib_urls[lib_index]}"
echo"Downloadingfrom${third_party_lib_urls[lib_index]}..."
echo${curl_cmd}
eval${curl_cmd}
if[-f${downloaded_archive_names[lib_index]}];then
echo"File${downloaded_archive_names[lib_index]}downloadedsuccessfully"

#Uncompressthearchivetoitsdesignateddirectory.
#Thestrip-componentsoptionwillensurethatallthefilesdirectlyendup
#inthedesireddirectory,withoutanyintermediatehierarchylevel.
tar_cmd="tar${tar_options[lib_index]}${downloaded_archive_names[lib_index]}-C${third_party_dir_names[lib_index]}--strip-components=1"
echo${tar_cmd}
echo"Extractingfrom${downloaded_archive_names[lib_index]}..."
eval${tar_cmd}

#Deletethecompressedarchive.
rm-rf${downloaded_archive_names[lib_index]}
else
echo"Errordownloadingfile${downloaded_archive_names[lib_index]}from${third_party_lib_urls[lib_index]}"
fi
done

#Backtothethird_partydirectory.
cd${THIRD_PARTY_DIR}

#Applygflagspatch.
patchgflags/CMakeLists.txt${PATCH_DIR}/gflags/CMakeLists.patch
patchgflags/src/gflags_reporting.cc${PATCH_DIR}/gflags/gflags_reporting.cc.patch

#Applyglogpatches.
patchglog/CMakeLists.txt${PATCH_DIR}/glog/glogCMakeLists.txt.patch
patchglog/src/utilities.cc${PATCH_DIR}/glog/utilities.cc.patch