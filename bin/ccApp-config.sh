# included in all the scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

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

# locate Java
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
  
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

# check for commoncrawl jar file ... 
if [ -z $CCAPP_JAR_PATH ]; then 

  if [ -e $CCAPP_HOME/lib/commoncrawl-*.jar ]; then
    CCAPP_JAR=`basename $CCAPP_HOME/lib/commoncrawl*.jar`
    CCAPP_JAR_PATH=$CCAPP_HOME/lib
    #echo "CCAPP_JAR_PATH:"$CCAPP_JAR_PATH
    
  elif [ -e $CCAPP_HOME/build/commoncrawl-*.jar ]; then
    CCAPP_JAR=`basename $CCAPP_HOME/build/commoncrawl*.jar`
    CCAPP_JAR_PATH=$CCAPP_HOME/build
    #echo "CCAPP_JAR_PATH:"$CCAPP_JAR_PATH
  fi
fi

if [ -z $CCAPP_JAR ]; then
  
  echo "ERROR:commoncrawl jar not found !!!"
  exit 1;

fi

#set some app defaults ... 
CCAPP_HOSTNAME=`hostname`
CCAPP_LOG_FILE=""
CCAPP_RUNLIB_PREFIX=""
CCAPP_LOG_DIR=""
CCAPP_RPC_INTERFACE=""
CCAPP_RPC_PORT=""
CCAPP_WEB_INTERFACE=""
CCAPP_WEB_PORT=""
CCAPP_VMARGS=""
CCAPP_DATA_DIR=""
CCAPP_HEAPSIZE=""
CCAPP_PID_DIR="/tmp/ccAppPIDS/"
CCAPP_NODEFAULTARGS=""
CCAPP_NOHUP=1
CCAPP_GDB=""
CCAPP_JAR_IN_CLASSPATH=0
CCAPP_IS_CC_SERVER=0

#check to see if the conf dir is given as an optional argument
while [ $# -gt 2 ]; do

    if [ "--config" = "$1" ]; then
        shift
	      confdir=$1
	      shift
	      CCAPP_CONF_DIR=$confdir
    elif [ "--hconfig" = "$1" ]; then
        shift
	      confdir=$1
	      shift
	      HADOOP_CONF_DIR=$confdir
    elif [ "--logdir" = "$1" ]; then
        shift
	      CCAPP_LOG_DIR=$1
	      shift
    elif [ "--logfile" = "$1" ]; then
        shift
	      CCAPP_LOG_FILE=$1
	      shift
    elif [ "--runlib" = "$1" ]; then
        shift
	      CCAPP_RUNLIB_PREFIX=$1
	      shift
    elif [ "--piddir" = "$1" ]; then
        shift
	      CCAPP_PID_DIR=$1
	      shift
    elif [ "--rootLogger" = "$1" ]; then
        shift
	      CCAPP_ROOT_LOGGER=$1
	      shift
    elif [ "--dataDir" = "$1" ]; then
        shift
	      CCAPP_DATA_DIR=$1
	      shift
    elif [ "--heapSize" = "$1" ]; then
        shift
	      CCAPP_HEAPSIZE=$1
	      shift
    elif [ "--hostname" = "$1" ]; then
        shift
	      CCAPP_HOSTNAME=$1
	      shift
    elif [ "--rpcInterface" = "$1" ]; then
        shift
	      CCAPP_RPC_INTERFACE=$1
	      shift
    elif [ "--rpcPort" = "$1" ]; then
        shift
	      CCAPP_RPC_PORT=$1
	      shift
    elif [ "--webInterface" = "$1" ]; then
        shift
	      CCAPP_WEB_INTERFACE=$1
	      shift
    elif [ "--webPort" = "$1" ]; then
        shift
	      CCAPP_WEB_PORT=$1
	      shift
    elif [ "--ccServer" = "$1" ]; then
        shift
              CCAPP_IS_CC_SERVER=1
    elif [ "--noDefaultArgs" = "$1" ]; then
        shift
	      CCAPP_NODEFAULTARGS=1
   	elif [ "--noJarInClassPath" = "$1" ]; then
        shift
	      CCAPP_JAR_IN_CLASSPATH=1
    elif [ "--gdbMode" = "$1" ]; then
        shift
	      CCAPP_NOHUP=""
	      CCAPP_GDB=1
    elif [ "--consoleMode" = "$1" ]; then
        shift
	      CCAPP_NOHUP=""
    elif [ "--p_arg0" = "$1" ]; then
        shift
        if [ -z "$CCAPP_ARGS" ]; then
          CCAPP_ARGS="$1"
        else
          CCAPP_ARGS="$CCAPP_ARGS $1"
        fi
        shift
    elif [ "--p_arg" = "$1" ]; then
        shift
        if [ -z "$CCAPP_ARGS" ]; then
          CCAPP_ARGS="$1 $2"
        else
          CCAPP_ARGS="$CCAPP_ARGS $1 $2"
        fi
	      shift
        shift
    elif [ "--vm_arg" = "$1" ]; then
        shift
        if [ -z "$CCAPP_VMARGS" ]; then
          CCAPP_VMARGS="$1"
        else
          CCAPP_VMARGS="$CCAPP_VMARGS $1"
        fi
	      shift
    else
      break
    fi
done

if [ -z $1 ]; then
  echo "action not specified (start/stop)"
  exit 1;
else
  CCAPP_ACTION=$1
  shift
fi
	
if [ -z $1 ]; then
  echo "app name not specified"
  exit 1;
else
  CCAPP_NAME=$1
  shift
fi

CCAPP_ARGS2=$@
echo ARGS2:$CCAPP_ARGS2
# Allow alternate conf dir location.
CCAPP_CONF_DIR="${CCAPP_CONF_DIR:-$CCAPP_HOME/conf}"

#define logdir and log file if not specified ... 
if [ "$CCAPP_LOG_DIR" = "" ]; then
  CCAPP_LOG_DIR=${CCAPP_HOME}/logs
fi

if [ "$CCAPP_DATA_DIR" = "" ]; then
  CCAPP_DATA_DIR=${CCAPP_HOME}/data
fi


if [ ! -d $CCAPP_LOG_DIR ]; then
  mkdir "$CCAPP_LOG_DIR"
fi

if [ ! -d $CCAPP_DATA_DIR ] ; then
  mkdir "$CCAPP_DATA_DIR"
fi

if [ "$CCAPP_LOG_FILE" = "" ]; then
  CCAPP_LOG_FILE=`python -c "l='${CCAPP_NAME}'.rsplit('.');print(l[len(l)-1])"` 
fi

if [ "$CCAPP_RUNLIB_PREFIX" = "" ]; then
  CCAPP_RUNLIB_PREFIX=`python -c "l='${CCAPP_NAME}'.rsplit('.');print(l[len(l)-1])"` 
fi


#identify os ... 
CCAPP_HOSTOS=`uname`
CCAPP_SQLITE_JARLOCATION=""
CCAPP_SQLITE_LIBRARYPATH=""
CCAPP_JNI_LIBRARYPATH="$CCAPP_HOME/lib"

case $CCAPP_HOSTOS in

  "Linux")
      CCAPP_SQLITE_JARLOCATION="$CCAPP_HOME/lib/sqlite.jar"
      CCAPP_SQLITE_LIBRARYPATH="$CCAPP_HOME/lib"
  ;;
  
  "Darwin")
      CCAPP_SQLITE_JARLOCATION="$JAVA_HOME/../Extensions/sqlite.jar"
      CCAPP_SQLITE_LIBRARYPATH="$JAVA_HOME/../Extensions"
  ;;
  
esac

if [ -z $CCAPP_SQLITE_JARLOCATION ]; then 
  echo "ERROR:Sqlite JAR not found!!!"
  exit 1
fi

# java parameters ... 
JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m 

# check envvars which might override default args
if [ "$CCAPP_HEAPSIZE" != "" ]; then
  JAVA_HEAP_MAX="-Xmx""$CCAPP_HEAPSIZE""m"
fi


#echo "JAVA:$JAVA"
#echo "CCAPP_HOME:$CCAPP_HOME"
#echo "CCAPP_CONF_DIR:$CCAPP_CONF_DIR"
#echo "CCAPP_NAME:$CCAPP_NAME"
#echo "CCAPP_ACTION:$CCAPP_ACTION"
#echo "CCAPP_VMARGS:$CCAPP_VMARGS"
#echo "CCAPP_JAR:$CCAPP_JAR"
#echo "CCAPP_JAR_PATH:$CCAPP_JAR_PATH"
#echo "CCAPP_LOG_DIR:$CCAPP_LOG_DIR"
#echo "CCAPP_LOG_FILE:$CCAPP_LOG_FILE"
#echo "CCAPP_RUNLIB_PREFIX:$CCAPP_RUNLIB_PREFIX"
#echo "CCAPP_ROOT_LOGGER:$CCAPP_ROOT_LOGGER"
#echo "CCAPP_HOSTNAME:$CCAPP_HOSTNAME"
#echo "CCAPP_DATA_DIR:$CCAPP_DATA_DIR"
#echo "CCAPP_HEAPSIZE:$CCAPP_HEAPSIZE"
#echo "CCAPP_SQLITE_JARLOCATION:$CCAPP_SQLITE_JARLOCATION"
#echo "CCAPP_PID_DIR:$CCAPP_PID_DIR"

#if [ "$CCAPP_NODEFAULTARGS" == "" ]; then
#	CCAPP_ARGS="$CCAPP_ARGS --hostname $CCAPP_HOSTNAME --dataDir $CCAPP_DATA_DIR"
#fi
 
if [ -n "$CCAPP_RPC_INTERFACE" ]; then 
  #echo "CCAPP_RPC_INTERFACE:$CCAPP_RPC_INTERFACE"
  CCAPP_ARGS="$CCAPP_ARGS --rpcIntfc $CCAPP_RPC_INTERFACE"  
fi
if [ -n "$CCAPP_RPC_PORT" ]; then 
  #echo "CCAPP_RPC_PORT:$CCAPP_RPC_PORT"
  CCAPP_ARGS="$CCAPP_ARGS --rpcPort $CCAPP_RPC_PORT"  
fi

if [ -n "$CCAPP_WEB_INTERFACE" ]; then 
  #echo "CCAPP_WEB_INTERFACE:$CCAPP_WEB_INTERFACE"
  CCAPP_ARGS="$CCAPP_ARGS --httpIntfc $CCAPP_WEB_INTERFACE"  
fi
if [ -n "$CCAPP_WEB_PORT" ]; then 
  #echo "CCAPP_WEB_PORT:$CCAPP_WEB_PORT"
  CCAPP_ARGS="$CCAPP_ARGS --httpPort $CCAPP_WEB_PORT"  
fi

#echo "CCAPP_ARGS:$CCAPP_ARGS"



# Try to locate hadoop home if not set ...  
if [ "$HADOOP_HOME" = "" ]; then
	#echo "HADOOP_HOME not defined. Attempting to locate via dcache.properties"
	export HADOOP_HOME=`cat $CCAPP_HOME/build.properties | grep "hadoop.path" | sed 's/.*=\(.*\)$/\1/'`
	
	if [ "$HADOOP_HOME" = "" ]; then
		echo "Failed to extract HADOOP_HOME from dcache.properties. Please set HADOOP_HOME to point to Hadoop Distribution"
		exit 1
	fi
fi

if [ -f ${HADOOP_HOME}/build/hadoop-core-*.jar ]; then
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


echo "HADOOP_HOME:$HADOOP_HOME"
echo "HADOOP_JAR:$HADOOP_JAR"
echo "HADOOP_CONF_DIR:$HADOOP_CONF_DIR"



