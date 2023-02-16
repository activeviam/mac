#! /bin/bash

# Script loading the result of an ActivePivot SandBox Memory Analysis Service's Export into a mac project 

# 1- Build the mac project as a sandbox jar
# 2- Load data from the input folder
# 3- Perform queries on the content of the loaded 


# This script EXPECTS to be executed from the ROOT of a buildable mac project	

# REQUIREMENTS :
# Java Development Kit 11+ (JRE is not enough as we use the jps command)
# Maven
# ActiveViam artifacts access
# Valid ActivePivot License
# cURL
# jq

# INPUT : 
# - 1 String : string containing the exported folder string
# - 2 Optional String : Path to the maven settings file, this file is expected to grant read access to ActiveViam artifacs

# OUTPUT : 
# - File : Server logs
# The server logs of both sandbox and mac applications as well as the maven outputs will be saved in the
# "logs" subfolder of the execution directory of the script


# Functions are defined to avoid command duplication and make the main script easily readable
# Make sure any variable used are defined before calling these
check_root(){
	#Just making sure the execution path is at the Root of a mac project
	if [[ -z $(cat pom.xml) || -z $(cat pom.xml | grep "<artifactId>${MAC_ARTIFACTID}</artifactId>") ]]; then
		echo "The execution directory of the script ${PWD} is not the root of a buildable mac project."
	exit 1
	fi	
}

check_requirements(){
	#Java
	#No version check as it's too vendor-dependent on the installed JDK
	if [ -z $(which java) ]; then
		echo "Java is not installed."
		exit 1
	#Maven
	elif [ -z $(which mvn) ]; then
		echo "Maven is not installed."
		exit 1
	#cURL
	elif [ -z $(which curl) ]; then
		echo "cURL is not installed."
		exit 1
	#jq
	elif [ -z $(which jq) ]; then
		echo "jq is not installed."
		exit 1
	fi
}

cleanup(){
	echo "Killing the java processes and removing temporary files..."
	kill -n 15 ${PID_MAC}
	rm -rdf ${BASE_DIR}/queries/output
}

# Function executing a remote call to the a loaded MAC cube available at localhost:9092
# Usage : check_query queryId
# queryId : String , Id of the checked query, its payload is expected to be available at ${BASE_DIR}/queries/input/${queryId}.json
#           and the values obtained for the cells to match the content of ${BASE_DIR}/queries/ref/${queryId}.txt
check_query(){
	if [ -z "$1" ];	then
		echo "No argument supplied to the query function"
		exit 1
    else
    	curl -X POST -u admin:admin -H "Content-Type:application/json" \
    		-d @${BASE_DIR}/queries/input/$1.json \
    		http://localhost:9092/activeviam/pivot/rest/v8/cube/query/mdx \
    		| jq .cells[].value > ${BASE_DIR}/queries/output/$1.txt
    fi

    if [ -z $(diff --strip-trailing-cr --ignore-all-space ${BASE_DIR}/queries/output/$1.txt ${BASE_DIR}/queries/ref/$1.txt) ]; then
    	echo $1"... OK"
    else
    	echo "Error when comparing expected query output to query result:"
    	echo $(diff --strip-trailing-cr --ignore-all-space ${BASE_DIR}/queries/output/$1.txt ${BASE_DIR}/queries/ref/$1.txt)
	     # Do the cleanup to make sure we don't leave open processes
	    cleanup
	    exit 1
	fi
}

###################
# MAIN SCRIPT START

if [ -z "$1" ]; then
    echo "No first argument supplied. Script usage : $0 export_folder [maven_settings_path]"
    exit 1
fi

echo "Read exported files at : $1"

if [ ! -z "$2" ]; then
	MAVEN_SETTINGS=$2
else
	MAVEN_SETTINGS=${PWD}/.circleci/circleci-settings.xml
fi

BASE_DIR=${PWD}/scripts
M2_PATH=$(mvn -s ${MAVEN_SETTINGS} help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
M2_UNIX=/$(echo "${M2_PATH}" | sed -e 's/\\/\//g' -e 's/://')
echo "Maven repository location: ${M2_UNIX}"
LOG_DIR=${PWD}/logs
mkdir -p ${LOG_DIR}
echo "Output logs folder location: ${LOG_DIR}"
BUILD_DIR=${PWD}/target

check_requirements

MAC_ARTIFACTID=$(mvn -s ${MAVEN_SETTINGS} help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
MAC_VERSION=$(mvn -s ${MAVEN_SETTINGS} help:evaluate -Dexpression=project.version -q -DforceStdout)

check_root

# 1- Execute the install goal to generate the MAC springBoot jar in the state of the repository

mvn -s ${MAVEN_SETTINGS} install -DskipTests=true > ${LOG_DIR}/maven.log
echo "Built the MAC app springboot JAR to ${BUILD_DIR}..."
echo

# Move the execution path of the mac app to the target folder to use avoid content service file locking
cd ${BUILD_DIR}
mkdir -p ./exported_statistics

java -jar ./${MAC_ARTIFACTID}-${MAC_VERSION}.jar --statistic.folder=exported_statistics > ${LOG_DIR}/mac.log&
cd ${PWD}
echo "Launched the MAC jar..."
echo

# Use jps to find the vmid matching the exact jar
VMID_MAC=$(jps -l | grep ${MAC_ARTIFACTID}-${MAC_VERSION}.jar | cut -d ' ' -f 1)
# Use ps to find the PID
PID_MAC=$(ps S | grep ${VMID_MAC} | xargs | cut -d ' ' -f 1)

# 2- Load files in MAC
cp -r $1 ${BUILD_DIR}/exported_statistics
echo "Pause the script for 30 seconds for the MAC data to be loaded (takes ~20 secs on CI VMs)..."
echo
sleep 30
echo "Resumed the script..."

# 3- Run queries on MAC & verify content
mkdir -p ${BASE_DIR}/queries/output
#Query 1 : COUNT Grand Total
check_query "query1"

# Cleanup
# Use the apps' PIDs to kill them
cleanup
