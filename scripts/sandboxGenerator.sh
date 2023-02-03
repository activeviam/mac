#! /bin/bash

# Script that executes the following operations :
# 1- Execute the repackage goal to generate the MAC springBoot jar in the state of the repository
# 2- Executes a maven goal that builds a customized (no real time) Sandbox springboot jar for a given version of Activepivot
# 3- Run both apps
# 4- Generate the memory statistics file for the Sandbox
# 5- Load the generated statisitics in the MAC app
# 6- Run queries on the MAC app
# 7- Kill both apps

# REQUIREMENTS :
# Java 11+
# Maven 3+
# ActiveViam artifacts access
# Valid ActivePivot License

# INPUT : 
# - String : AP version

# OUTPUT : 
# - File : Query logs

# 0- Log execution path & script location path
echo
echo "Script executed from: ${PWD}"
BASEDIR=$(dirname $0)
echo "Script location: ${BASEDIR}"
M2_PATH=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
M2_UNIX=/$(echo "${M2_PATH}" | sed -e 's/\\/\//g' -e 's/://')

echo "Maven repository location: ${M2_UNIX}"
echo
# 1- Execute the install goal to generate the MAC springBoot jar in the state of the repository

mvn clean install -DskipTests=true > maven.log

echo "Built the MAC app springboot JAR..."
echo

# 2- Obtain a sandbox jar 
# Since the current environment already has java and some dependencies, a docker container sounds overkill
# For now just get sandbox-activepivot-X.Y.Z.jar from activepivot-mvn-nightly repository in the .m2 repo
mvn org.apache.maven.plugins:maven-dependency-plugin:2.1:get \
    -DrepoUrl=https://activeviam.jfrog.io/artifactory/activepivot-mvn-nightly \
    -Dartifact=com.activeviam.sandbox:sandbox-activepivot:6.0.0 > maven.log

echo "Downloaded the sandbox jar in the m2 repo..."
echo

# extract the csv files in ./data
rm -rdf ./data
unzip -j ${M2_PATH}"/com/activeviam/sandbox/sandbox-activepivot/6.0.0""/sandbox-activepivot-6.0.0.jar" 'BOOT-INF/classes/data/*' -d ./data/> /dev/null

# 3- Run both apps

java -jar ${M2_PATH}"/com/activeviam/sandbox/sandbox-activepivot/6.0.0""/sandbox-activepivot-6.0.0.jar" --csvSource.dataset=./data --tradeSource.timerDelay=1000000000 --ratings.random=false --risks.random=false>sandbox.log&

echo "Extracted CSV files and launched the sandbox jar..."
echo

# Move the execution path of the mac app to somewhere else to use another content service file
cd target
mkdir ./exported_statistics
java -jar ./mac-3.0.0-SNAPSHOT.jar --statistic.folder=exported_statistics> ../mac.log&
cd ../
echo "Launched the MAC jar..."
echo

# Use jps to find the vmid matching the exact jar
VMID_SANDBOX=$(jps -l | grep sandbox-activepivot-6.0.0.jar | cut -d ' ' -f 1)
VMID_MAC=$(jps -l | grep mac-3.0.0-SNAPSHOT.jar | cut -d ' ' -f 1)

# Use ps to find the PID
PID_SANDBOX=$(ps S | grep ${VMID_SANDBOX} | xargs | cut -d ' ' -f 1)
PID_MAC=$(ps S | grep ${VMID_MAC} | xargs | cut -d ' ' -f 1)

# 4- Generate the memory statistics file for the Sandbox

# Use jxmterm to access the mbean
curl -OL https://github.com/jiaqi/jmxterm/releases/download/v1.0.4/jmxterm-1.0.4-uber.jar

echo "Downloaded the jmxterm uber-jar..."
echo

# Generate a script file with correct vmid for rmid connection
rm ${BASEDIR}/jmxtermCommands.txt

touch ${BASEDIR}/jmxtermCommands.txt

echo open ${VMID_SANDBOX} >> ${BASEDIR}/jmxtermCommands.txt
echo bean com.activeviam:node0=MemoryAnalysisService >>  ${BASEDIR}/jmxtermCommands.txt
echo run Dump\\ memory\\ statistics folder >>  ${BASEDIR}/jmxtermCommands.txt
echo exit >>  ${BASEDIR}/jmxtermCommands.txt

echo "Pause the script for 60 seconds for the Sandbox App to be ready to export stats ..."
echo
sleep 60
echo "Resumed the script..."
echo

# run jmxterm in non-interactive mode with the script file
java -jar jmxterm-1.0.4-uber.jar -n -o jmxterm.log < ${BASEDIR}/jmxtermCommands.txt

echo "Ran the export MBean..."
echo

# 5- Load files in MAC
cp -r $(cat jmxterm.log) ./target/exported_statistics
echo "Pause the script for 10 seconds for the MAC data to be loaded ..."
echo
sleep 10
echo "Resumed the script..."
echo

# 6- Run queries on MAC
# TODO

# 7- Cleanup
# Use the apps' PIDs to kill them
echo "Killing the java processes..."
kill -n 15 ${PID_SANDBOX}
kill -n 15 ${PID_MAC}