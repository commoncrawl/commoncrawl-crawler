#!/usr/bin/env bash

# resolve links - $0 may be a softlink

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done


# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the app installation
export CCAPP_HOME=`dirname "$this"`/..

#echo "CCAPP_HOME:"$CCAPP_HOME
#echo "CCAPP_CONF_DIR:$CCAPP_CONF_DIR"
#echo "CCAPP_LOG_DIR:$CCAPP_LOG_DIR"

if ! [ -e $CCAPP_HOME/build/commoncrawl-*.jar ]; then
	echo "Please build dcache jar"
else
	CCAPP_JAR=`basename $CCAPP_HOME/build/commoncrawl*.jar`
	CCAPP_JAR_PATH=$CCAPP_HOME/build
	#echo "CCAPP_JAR:"$CCAPP_JAR
	#echo "CCAPP_JAR_PATH:"$CCAPP_JAR_PATH
fi	

if [ "$JAVA_HOME" = "" ]; then
  if [ -d /usr/lib/jvm/java-6-sun ]; then
    export JAVA_HOME=/usr/lib/jvm/java-6-sun
  elif [ -e /usr/libexec/java_home ]; then
    export JAVA_HOME=`/usr/libexec/java_home`
  fi
fi

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

if [ $# -lt 1 ]; then
  echo "usage launchtest.sh <test class>"
  exit 1;
fi
  

#extract main class name 
CCAPP_CLASS_NAME=$1

if [ "$CCAPP_CLASS_NAME" = "" ]; then
	echo "No Main Class Specified!"
	exit 1;
fi
shift


# parse config parameter 
CCAPP_CONFIG=dcache_tests

CCAPP_RUNTIME_DIR=/tmp

rm -r -f $CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/*
mkdir -p $CCAPP_RUNTIME_DIR/$CCAPP_CONFIG

#capture absolute path to runtime dir 
ORIG_DIR=`pwd`
cd $CCAPP_RUNTIME_DIR
CCAPP_RUNTIME_DIR=`pwd`
cd $ORIG_DIR

#extract optional arguements 
JAVA_MAX_HEAP=2000m
JAVA_NEW_HEAP=200m
CCREAL_APP_NAME=""
CCAPP_ROOT_LOGGER="-INFO,DRFA,console"

while [ $# ]; do

  if [ "--MaxHeap" = "$1" ]; then
    JAVA_MAX_HEAP=$2
    shift 2
  elif [ "--NewHeap" = "$1" ]; then
    JAVA_NEW_HEAP=$2
    shift 2
  elif [ "--AppName" = "$1" ]; then
    CCREAL_APP_NAME=$2
    CCAPP_NAME=$CCREAL_APP_NAME
    shift 2
  elif [ "--nohup" = "$1" ]; then
    NOHUP=1
    CCAPP_ROOT_LOGGER="-INFO,DRFA"
    shift
  else
    break
  fi
done
#echo "class:"$CCAPP_CLASS_NAME

CCAPP_RUNTIME_APP_ROOT=$CCAPP_RUNTIME_DIR/$CCAPP_CONFIG
#echo "**runtime root:"$CCAPP_RUNTIME_APP_ROOT

if [ ! -d $CCAPP_RUNTIME_APP_ROOT ];then mkdir $CCAPP_RUNTIME_APP_ROOT; fi
if [ ! -d $CCAPP_RUNTIME_APP_ROOT/conf ];then  mkdir $CCAPP_RUNTIME_APP_ROOT/conf; fi
if [ ! -d $CCAPP_RUNTIME_APP_ROOT/static_data ];then  mkdir $CCAPP_RUNTIME_APP_ROOT/static_data; fi
if [ ! -d $CCAPP_RUNTIME_APP_ROOT/data ];then  mkdir $CCAPP_RUNTIME_APP_ROOT/data; fi
if [ ! -d $CCAPP_RUNTIME_APP_ROOT/logs ];then  mkdir $CCAPP_RUNTIME_APP_ROOT/logs; fi
if [ ! -d $CCAPP_RUNTIME_APP_ROOT/pids ];then  mkdir $CCAPP_RUNTIME_APP_ROOT/pids; fi

#copy config data to runtime dir 

#cp -R $CCAPP_HOME/runtime_conf/$CCAPP_CONFIG/conf/* $CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/conf
#cp -R $CCAPP_HOME/runtime_conf/$CCAPP_CONFIG/static_data $CCAPP_RUNTIME_DIR/$CCAPP_CONFIG

CCAPP_LOG_DIR=$CCAPP_RUNTIME_APP_ROOT/logs


#echo "CCAPP_CLASS_NAME:$CCAPP_CLASS_NAME"
if [ -z $CCAPP_NAME ]; then
  CCAPP_NAME=`echo $CCAPP_CLASS_NAME | sed 's/.*\.\(.*\)$/\1/'`
fi
#echo "CCAPP_NAME:$CCAPP_NAME"

if [ -d "$CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/runlib_$CCAPP_NAME" ]; then 
  rm -r -f "$CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/runlib_$CCAPP_NAME"
fi

# extract jar 
mkdir "$CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/runlib_$CCAPP_NAME"
ORIG_LOC=`pwd`
#echo "**entering $CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/runlib_$CCAPP_NAME"
cd "$CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/runlib_$CCAPP_NAME"
#echo "copying jar to $CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/runlib_$CCAPP_NAME"
cp ${CCAPP_JAR_PATH}/${CCAPP_JAR} $CCAPP_RUNTIME_DIR/$CCAPP_CONFIG/runlib_$CCAPP_NAME
#echo "extracting jar"
# now extract the jar ... 
jar -xf ${CCAPP_JAR_PATH}/${CCAPP_JAR}
cd $ORIG_LOC

if [ "$HADOOP_HOME" = "" ]; then
	#echo "HADOOP_HOME not defined. Attempting to locate via dcache.properties"
	export HADOOP_HOME=`cat $ORIG_DIR/build.properties | grep "hadoop.path" | sed 's/.*=\(.*\)$/\1/'`
	
	if [ "$HADOOP_HOME" = "" ]; then
		echo "Failed to extract HADOOP_HOME from dcache.properties. Please set HADOOP_HOME to point to Hadoop Distribution"
		exit 1
	fi
fi

if [ -f $HADOOP_HOME/build/hadoop-*-core.jar ]; then 
    HADOOP_JAR=`ls $HADOOP_HOME/build/hadoop-*-core.jar`
elif [ -f $HADOOP_HOME/hadoop-*-core.jar ]; then
    HADOOP_JAR=`ls $HADOOP_HOME/hadoop-*-core.jar`
elif [ -f ${HADOOP_HOME}/build/hadoop-core-*.jar ]; then
    HADOOP_JAR=`ls $HADOOP_HOME/build/hadoop-core-*.jar`
elif [ -f ${HADOOP_HOME}/hadoop-core-*.jar ]; then
    HADOOP_JAR=`ls $HADOOP_HOME/hadoop-core-*.jar`
fi

if [ "$HADOOP_JAR" = "" ]; then
  echo "Unable to locate hadoop core jar file. Please check your hadoop installation."
  exit 1
fi

if [ "$HADOOP_CONF_DIR" = "" ]; then
	HADOOP_CONF_DIR="$HADOOP_HOME/conf"
fi

#if [ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]; then
#  ${HADOOP_CONF_DIR}/hadoop-env.sh
#fi

# CLASSPATH initially contains CCAPP_CONF:HADOOP_CONF_DIR
CLASSPATH=$CCAPP_RUNTIME_APP_ROOT/runlib_$CCAPP_NAME/conf:$CCAPP_RUNTIME_APP_ROOT/conf:$CCAPP_RUNTIME_APP_ROOT/static_data
# add in hadoop config
CLASSPATH=${CLASSPATH}:${HADOOP_CONF_DIR}
# and hbase config
#CLASSPATH=${CLASSPATH}:${HBASE_CONF_DIR}
# add in web app class path 
CLASSPATH=${CLASSPATH}:$CCAPP_RUNTIME_APP_ROOT/runlib_$CCAPP_NAME/webapps
# and add in test path ... 
CLASSPATH=${CLASSPATH}:$CCAPP_RUNTIME_APP_ROOT/runlib_$CCAPP_NAME/tests
# next add tools.jar
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
# next add dcache jar FIRST ... 
CLASSPATH=${CLASSPATH}:$CCAPP_RUNTIME_APP_ROOT/runlib_$CCAPP_NAME/${CCAPP_JAR}
# then add nested libraries in dcache jar
for f in $CCAPP_RUNTIME_APP_ROOT/runlib_$CCAPP_NAME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
#next add hadoop jar path 
CLASSPATH=${CLASSPATH}:${HADOOP_JAR}
# add hadoop libs to CLASSPATH
for f in $HADOOP_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
# and add jetty libs ... 
for f in $HADOOP_HOME/lib/jetty-ext/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

export CLASSPATH=$CLASSPATH
#echo "";
#echo "CLASSPATH:$CLASSPATH"
#echo "";

# change to runtime config directory ... 
cd $CCAPP_HOME/runtime_conf/$CCAPP_CONFIG

JAVA="$JAVA_HOME/bin/java"

#establish library path 
CCAPP_LIB_DIR=$CCAPP_RUNTIME_APP_ROOT/runlib_$CCAPP_NAME/lib
#establish hadoop platform name string 
JAVA_PLATFORM=`CLASSPATH=${CLASSPATH} ${JAVA} org.apache.hadoop.util.PlatformName | sed -e 's/ /_/g' | sed -e "s/ /_/g"`
#echo Platform Name is:${JAVA_PLATFORM}
#setup commoncrawl library paths
#TODO:HACKED IN MozillaParser location for now
export JAVA_LIBRARY_PATH=${CCAPP_LIB_DIR}:${CCAPP_LIB_DIR}/native/${JAVA_PLATFORM}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}:/usr/local/MozillaParser-v-0-3-0/dist/linux/x86_64
#echo "**JAVA_LIBRARY_PATH:"$JAVA_LIBRARY_PATH
#TODO:HACKED IN FOR NOW 
export MOZILLA_LIB_PATH=/usr/local/MozillaParser-v-0-3-0/dist/linux/x86_64
#setup execution path 
export PATH=${CCAPP_LIB_DIR}/native/${JAVA_PLATFORM}:$PATH
#echo "**PATH:"$PATH
#and ld_library path 
export LD_LIBRARY_PATH=${CCAPP_LIB_DIR}/native/${JAVA_PLATFORM}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}:$LD_LIBRARY_PATH
#TODO:HACK IN MOZILLA PARSER PATH FOR NOW 
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/MozillaParser-v-0-3-0/dist/linux/x86_64:/usr/local/MozillaParser-v-0-3-0/dist/linux/x86_64/mozilla
echo "**LD_LIBRARY_PATH:"${LD_LIBRARY_PATH}
if [ -z $CCREAL_APP_NAME ]; then
  if [ $CCAPP_NAME = "CommonCrawlServer" ] ; then
  	GET_REALAPP_NAME_CMD="$JAVA -classpath $CLASSPATH $CCAPP_CLASS_NAME $@ --dumpAppName"
  	CCREAL_APP_NAME=`$GET_REALAPP_NAME_CMD`
  	#echo "Real app name is:" $CCAPP_NAME
	CCAPP_NAME=$CCREAL_APP_NAME
  fi
fi

if [ -z $CCAPP_NAME ]; then
	echo "Unable to retrieve app name!"
	exit 1
fi
#set log file name 
CCAPP_LOG_FILE=$CCAPP_NAME.log
#set pid file path 
pidfile=$CCAPP_RUNTIME_APP_ROOT/pids/${CCAPP_NAME}.pid
#check it is not running
EXISTS="" 
if [ -e $pidfile ]; then
  old_pid=`cat $pidfile`
  EXISTS=""
  if [ -e /proc ]; then 
    if [ -e /proc/$old_pid ]; then
      EXISTS=1
    fi
  else
    EXISTS=`ps -p $old_pid | grep $old_pid | sed 's/[^0-9]*\([0-9]*\).*$/\1/'`
  fi
  
  if ! [ -z $EXISTS ]; then
    echo "*************************"
    echo "$CCAPP_NAME already running as PID:$old_pid"
    echo "*************************"
    echo "BYE!"
    echo ""
    exit 1;
  fi
fi 

	
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.log.dir=$CCAPP_LOG_DIR"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.log.file=$CCAPP_LOG_FILE"
CCAPP_VMARGS="$CCAPP_VMARGS -Dhadoop.home.dir=$HADOOP_HOME"
CCAPP_VMARGS="$CCAPP_VMARGS -Ddcache.runtime.root=$CCAPP_RUNTIME_APP_ROOT"
CCAPP_VMARGS="$CCAPP_VMARGS -Ddcache.config.name=$CCAPP_CONFIG"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.root.logger=${CCAPP_ROOT_LOGGER:-INFO,DRFA}"
CCAPP_VMARGS="$CCAPP_VMARGS -Dlog4j.configuration=log4j.properties"
CCAPP_VMARGS="$CCAPP_VMARGS -Xmx$JAVA_MAX_HEAP"
CCAPP_VMARGS="$CCAPP_VMARGS -XX:NewSize=$JAVA_NEW_HEAP"
CCAPP_VMARGS="$CCAPP_VMARGS -XX:+UseParNewGC -XX:ParallelGCThreads=8 -XX:+PrintGCDetails"
CCAPP_VMARGS="$CCAPP_VMARGS -Djava.library.path=${JAVA_LIBRARY_PATH}"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcc.native.lib.path=${CCAPP_LIB_DIR}/native/${JAVA_PLATFORM}"

CCAPP_CMD_LINE="$JAVA $CCAPP_VMARGS org.junit.runner.JUnitCore $CCAPP_CLASS_NAME $@"
CCAPP_RUN_LOG=$CCAPP_LOG_DIR/${CCAPP_NAME}_run.log
$CCAPP_CMD_LINE | tee $CCAPP_RUN_LOG

