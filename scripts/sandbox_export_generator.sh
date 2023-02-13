#! /bin/bash

# Script Exporting the Memory Analysis Service output of a released ActivePivot sandbox

# 1- Executes a maven goal that builds a customized (no real time) Sandbox springboot jar for a given version of Activepivot
# 2- Run the sandbox
# 3- Generate the memory statistics file for the Sandbox

# This script EXPECTS to be executed from the ROOT of a buildable mac project	

# REQUIREMENTS :
# Java Development Kit 11+ (JRE is not enough as we use the jps command)
# Maven
# ActiveViam artifacts access
# Valid ActivePivot License
# cURL

# INPUT : 
# - 1 String : ActivePivot version used by the sandbox app
# - 2 String : URL of the Artifactory Sandbox repository
# - 3 Optional String : Path to the maven settings file, this file is expected to grant read access to ActiveViam artifacs

# OUTPUT : exportFolder
# - Files : Server logs
# The server logs of both sandbox and mac applications as well as the maven outputs will be saved in the
# "logs" subfolder of the execution directory of the script
# - Files : Memory Export

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
	fi
}

cleanup(){
	echo "Killing the java processes and removing temporary files..."
	kill -n 15 ${PID_SANDBOX}
	rm jmxterm-${JMXTERM_VERSION}-uber.jar
	rm ${BASE_DIR}/jmxtermCommands.txt
	rm -rdf ${BASE_DIR}/queries/output
	rm -rdf ${SANDBOX_DATA_DIR}
}

###################
# MAIN SCRIPT START

if [ -z "$1" ]; then
    echo "No first argument supplied. Script usage : $0 sandbox_version repository_url [maven_settings_path]"
    exit 1
elif [ -z "$2" ]; then
    echo "No second argument supplied. Script usage : $0 sandbox_version repository_url [maven_settings_path]"
    exit 1
fi

if [ ! -z "$3" ]; then
	MAVEN_SETTINGS=$3
else
	MAVEN_SETTINGS=${PWD}/.circleci/circleci-settings.xml
fi

AP_VERSION=$1
AP_ARTIFACTORY_URL=$2

AP_REPO_PATH=/com/activeviam/sandbox/sandbox-activepivot/

echo "Script executed from: ${PWD} for ActivePivot version ${AP_VERSION}"
BASE_DIR=${PWD}/scripts
M2_PATH=$(mvn -s ${MAVEN_SETTINGS} help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
M2_UNIX=/$(echo "${M2_PATH}" | sed -e 's/\\/\//g' -e 's/://')
echo "Maven repository location: ${M2_UNIX}"
LOG_DIR=${PWD}/logs
mkdir -p ${LOG_DIR}
echo "Output logs folder location: ${LOG_DIR}"
BUILD_DIR=${PWD}/target
SANDBOX_DATA_DIR=${PWD}/sandbox_data


check_requirements

MAC_ARTIFACTID=$(mvn -s ${MAVEN_SETTINGS} help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
MAC_VERSION=$(mvn -s ${MAVEN_SETTINGS} help:evaluate -Dexpression=project.version -q -DforceStdout)

check_root

JMXTERM_VERSION=1.0.4
JMX_REPO_PATH=https://github.com/jiaqi/jmxterm/releases/download/
JMX_JAR_PATH=${JMX_REPO_PATH}v${JMXTERM_VERSION}/jmxterm-${JMXTERM_VERSION}-uber.jar

# 2- Obtain a sandbox jar 
# Since the current environment already has java and some maven dependencies, a docker container sounds overkill
# For now just get sandbox-activepivot-X.Y.Z.jar from activepivot-mvn-nightly repository in the .m2 repo
mvn -s ${MAVEN_SETTINGS} org.apache.maven.plugins:maven-dependency-plugin:2.1:get \
    -DrepoUrl=${AP_ARTIFACTORY_URL} \
    -Dartifact=com.activeviam.sandbox:sandbox-activepivot:${AP_VERSION} >> ${LOG_DIR}/maven.log

echo "Downloaded the ${AP_VERSION} sandbox jar in the m2 repo..."
echo
# extract the csv files in SANDBOX_DATA_DIR
unzip -o -q -j ${M2_PATH}${AP_REPO_PATH}${AP_VERSION}"/sandbox-activepivot-"${AP_VERSION}".jar" 'BOOT-INF/classes/data/*' -d ${SANDBOX_DATA_DIR}/

# 3- Run both apps

java -jar ${M2_PATH}${AP_REPO_PATH}${AP_VERSION}"/sandbox-activepivot-"${AP_VERSION}".jar" --csvSource.dataset=${SANDBOX_DATA_DIR} --tradeSource.timerDelay=1000000000 --ratings.random=false --risks.random=false>logs/sandbox.log&

echo "Extracted CSV files and launched the sandbox jar..."
echo

# Use jps to find the vmid matching the exact jar
VMID_SANDBOX=$(jps -l | grep sandbox-activepivot-${AP_VERSION}.jar | cut -d ' ' -f 1)
# Use ps to find the PID
PID_SANDBOX=$(ps S | grep ${VMID_SANDBOX} | xargs | cut -d ' ' -f 1)

# 4- Generate the memory statistics file for the Sandbox

# Download the jmxterm jar
curl -s -OL ${JMX_JAR_PATH} > ${LOG_DIR}/curl.log
# Generate a script file with correct vmid for rmid connection
touch ${BASE_DIR}/jmxtermCommands.txt
# Use jxmterm to access the mbean
echo open ${VMID_SANDBOX} >> ${BASE_DIR}/jmxtermCommands.txt
echo bean com.activeviam:node0=MemoryAnalysisService >>  ${BASE_DIR}/jmxtermCommands.txt
echo run Dump\\ memory\\ statistics folder >>  ${BASE_DIR}/jmxtermCommands.txt
echo exit >>  ${BASE_DIR}/jmxtermCommands.txt
	echo "Pause the script for 60 seconds for the Sandbox App to be ready to export stats ..."
echo
sleep 60
echo "Resumed the script..."
echo
# run jmxterm in non-interactive mode with the script file
java -jar jmxterm-${JMXTERM_VERSION}-uber.jar -n -o ${LOG_DIR}/jmxterm.log < ${BASE_DIR}/jmxtermCommands.txt
echo "Ran the export MBean..."
# Ensure the mbean call succeeded
if [ -z "$(cat ${LOG_DIR}/jmxterm.log)" ]; then
     echo "No output in the export MBean execution, something went wrong. Aborting."
     # Do the cleanup to make sure we don't leave open processes
     cleanup
     exit 1
fi
cleanup