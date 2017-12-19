#! /bin/bash

set -e

##### VARIABLES #####
SCRIPT=`basename ${BASH_SOURCE[0]}`
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P )"

INSTALL_DIR=/home/$USER
DEFAULT_MESOS_BUILD_HOME="$INSTALL_DIR/mesos-build-home"

HADOOP_VERSION=
MESOS_GIT_CHECKOUT_STR=
SPARK_GIT_CHECKOUT_STR=
SPARK_GIT_REPO="https://github.com/lightbend/spark.git"
SBT_EXIT_CODE=

INTEGRATION_TESTS_PATH="/home/$USER/mesos-spark-integration-tests"

##### FUNCTIONS ######

function build_mesos {
  M_BUILD_HOME=

  if [[ -n $MESOS_BUILD_HOME  ]]; then
    M_BUILD_HOME=$MESOS_BUILD_HOME
  else
    M_BUILD_HOME=$DEFAULT_MESOS_BUILD_HOME
  fi

  cd $M_BUILD_HOME

  if [[ -n $MESOS_GIT_CHECKOUT_STR ]]; then
    git pull origin master
    git checkout $MESOS_GIT_CHECKOUT_STR
  fi

  ./bootstrap
  rm -rf build
  # Configure and build.
  mkdir -p build
  cd build
  ../configure
  make clean
  make

  export MESOS_NATIVE_JAVA_LIBRARY="$M_BUILD_HOME"/build/src/.libs/libmesos.so
}

function build_spark {
  export JAVA_HOME=$(readlink -f $(which java) | sed "s:bin/java::")
  SCALA_VERSION_PARAM=
  if [[ -n $MIT_SCALA_VERSION ]]; then
    SPARK_BUILD_SCALA_VERSION=${MIT_SCALA_VERSION%.*}
    ${SPARK_BUILD_HOME}/dev/change-scala-version.sh $SPARK_BUILD_SCALA_VERSION
    SCALA_VERSION_PARAM="-Dscala-$SPARK_BUILD_SCALA_VERSION"
  fi

  ${SPARK_BUILD_HOME}/build/mvn $SPARK_BUILD_EXTRA_OPTIONS -Pmesos -Phadoop-"${HADOOP_VERSION%.*}" $SCALA_VERSION_PARAM -Dhadoop.version="$HADOOP_VERSION" -DskipTests clean package

  if [[ -f "$SPARK_BUILD_HOME/make-distribution.sh" ]]; then
      MAKE_DISTRO_PATH="$SPARK_BUILD_HOME/make-distribution.sh"
  else
    if [[ -f "$SPARK_BUILD_HOME/dev/make-distribution.sh" ]]; then
      MAKE_DISTRO_PATH="$SPARK_BUILD_HOME/dev/make-distribution.sh"
    else
      echo "make-distribution.sh does not exist..."
      exit 1
    fi
  fi

  $MAKE_DISTRO_PATH --name test --tgz -Pmesos -Phadoop-"${HADOOP_VERSION%.*}" $SCALA_VERSION_PARAM
}

function start_docker_cluster {
  stop_docker_cluster

  rm -rf $INTEGRATION_TESTS_PATH
  cd /home/$USER
  git clone https://github.com/lightbend/mesos-spark-integration-tests.git

  BUILD_SBT=${INTEGRATION_TESTS_PATH}/test-runner/build.sbt
  if [[ -n $MIT_SCALA_VERSION ]]; then
    sed -i 's/scalaVersion := .*/scalaVersion := "'"$MIT_SCALA_VERSION"'"/g' $BUILD_SBT
  fi

  # get hadoop binary file

  HADOOP_BIN_HOME="/home/$USER/hadoop-bin"
  TMP_FILE_PATH="hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"
  HADOOP_BIN_FILE="$HADOOP_BIN_HOME/hadoop-$HADOOP_VERSION.tar.gz"
  rm -f $HADOOP_BIN_FILE
  wget -q -P $HADOOP_BIN_HOME "http://mirror.switch.ch/mirror/apache/dist/hadoop/common/$TMP_FILE_PATH"
  # path to spark binary file
  SPARK_FILE="$(ls ${SPARK_BUILD_HOME}/*.tgz)"

  local BASE=${INTEGRATION_TESTS_PATH}/mesos-docker/run
  if [[ -n $CLUSTER_CUSTOM_OPTIONS ]]; then
    $BASE/run.sh --spark-binary-file $SPARK_FILE --hadoop-binary-file $HADOOP_BIN_FILE $CLUSTER_CUSTOM_OPTIONS
  else
    $BASE/run.sh --mesos-master-config "--roles=spark_role" \
    --mesos-slave-config "--resources=disk(spark_role):10000;cpus(spark_role):1;mem(spark_role):2000;cpus(*):2;mem(*):2000;disk(*):10000" \
    --number-of-slaves 2 --spark-binary-file $SPARK_FILE --hadoop-binary-file $HADOOP_BIN_FILE
  fi
}

function stop_docker_cluster {
  local BASE=${INTEGRATION_TESTS_PATH}/mesos-docker/run
  if [ -d "$BASE" ]; then
    $BASE/cluster_remove.sh
  fi
}

function execute_test_runner {
  local BASE=${INTEGRATION_TESTS_PATH}/test-runner
  SPARK_FILE="$(ls ${SPARK_BUILD_HOME}/*.tgz)"
  # unzip spark tgz file with current dir
  cd "$DIR"
  tar -xvf $SPARK_FILE

  # gets the extracted folder name
  SPARK_EXT_DIR=${SPARK_FILE%.*}
  # sets the full path to the extracted folder name
  SPARK_EXT_DIR=${DIR}/${SPARK_EXT_DIR##*/}
  cd $BASE
  echo "SPARK_EXT_DIR=$SPARK_EXT_DIR"
  sbt -Dspark.home=$SPARK_EXT_DIR -Dconfig.file="mit-application.conf" clean "mit $SPARK_EXT_DIR mesos://172.17.0.1:5050"
  SBT_EXIT_CODE=$?
}

function show_help {

  cat<< EOF
  This script builds spark, creates a mini mesos cluster for testing purposes.
  Usage: $SCRIPT [OPTIONS]

  eg: ./$SCRIPT --hadoop-version 2.7.4

  Options:

  -h|--help prints this message.
  --hadoop-version the hadoop version to use eg. 2.6
  --mesos-git-checkout-str the git checkout string to pass to get for example a specific branch
  --s3-bucket if present spark distro will be pushed
  --spark-build-extra-options extra options to pass at build process for spark
  --spark-build-home full path where spark repo is checkedout
EOF

}

function parse_args {
  #parse args
  while :; do
    case $1 in
      -h|--help)   # Call a "show_help" function to display a synopsis, then exit.
      show_help
      exit
      ;;
      --hadoop-version)       # Takes an option argument, ensuring it has been specified.
      if [ -n "$2" ]; then
        export HADOOP_VERSION=$2
        shift 2
        continue
      else
        exitWithMsg '"--hadoop-version" requires a non-empty option argument.\n'
      fi
      ;;
      --spark-build-extra-options)       # Takes an option argument, ensuring it has been specified.
      if [ -n "$2" ]; then
        export SPARK_BUILD_EXTRA_OPTIONS=$2
        shift 2
        continue
      else
        exitWithMsg '"--spark-build-extra-options" requires a non-empty option argument.\n'
      fi
      ;;
      --spark-build-home)       # Takes an option argument, ensuring it has been specified.
      if [ -n "$2" ]; then
        export SPARK_BUILD_HOME=$2
        shift 2
        continue
      else
        exitWithMsg '"--spark-build-home" requires a non-empty option argument.\n'
      fi
      ;;
      --s3-bucket)       # Takes an option argument, ensuring it has been specified.
      if [ -n "$2" ]; then
        export S3_BUCKET=$2
        shift 2
        continue
      else
        exitWithMsg '"--s3-bucket" requires a non-empty option argument.\n'
      fi
      ;;
      --)              # End of all options.
      shift
      break
      ;;
      --mesos-git-checkout-str)       # Takes an option argument, ensuring it has been specified.
      if [ -n "$2" ]; then
        MESOS_GIT_CHECKOUT_STR=$2
        shift 2
        continue
      else
        exitWithMsg '"--spark-git-checkout-str" requires a non-empty option argument.\n'
      fi
      ;;
      --)              # End of all options.
      shift
      break
      ;;
      -?*)
      printf 'The option is not valid...: %s\n' "$1" >&2
      show_help
      exit 1
      ;;
      *)               # Default case: If no more options then break out of the loop.
      break
    esac
    shift
  done

  if [ -z "$SPARK_BUILD_HOME" ]; then
    SPARK_BUILD_HOME="$DIR/../"
  fi
}

function exitWithMsg {
  printf 'ERROR: '"$1"'.\n' >&2
  show_help
  exit 1
}

#
# Returns a string of the command to upgrade or downgrade mesos
# $1 version to upgrade or downgrade to
#
function update_mesos_str {
  local RET="sudo apt-get update -o Dir::Etc::sourcelist=sources.list.d/mesosphere.list -o Dir::Etc::sourceparts=- -o APT::Get::List-Cleanup=0"
  m_version="$(sudo apt-cache policy mesos | sed -n -e '/Version table:/,$p' | sed 's/\*\*\*//g' | grep "$1" | awk {'print $1'} | head -1)"
  RET="$RET; sudo apt-get --yes --force-yes install mesos=$m_version"
  echo $RET
}
##### main #####

parse_args "$@"
#build_mesos
#update mesos on host
sudo apt-get -qq update
if [[ -n $MESOS_HOST_LIB_VERSION ]]; then
  com=$(update_mesos_str $MESOS_HOST_LIB_VERSION)
  eval $com
else
  sudo apt-get --yes --force-yes install --only-upgrade mesos
fi
build_spark
start_docker_cluster
execute_test_runner
stop_docker_cluster

if [[ -z $SBT_EXIT_CODE ]]; then
  #something is wrong sbt never run
  SBT_EXIT_CODE=1
fi

if [ -n "$S3_BUCKET" ] && [ $SBT_EXIT_CODE -eq 0 ]; then
  # push artifact
  aws s3 cp $SPARK_FILE s3://${S3_BUCKET}/${SPARK_FILE##*/} --acl public-read
fi

rc=

echo "exiting from $SCRIPT with code...$SBT_EXIT_CODE"
exit $SBT_EXIT_CODE

