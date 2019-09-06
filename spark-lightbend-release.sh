#!/usr/bin/env bash
set -ex

function error {
  echo "$*"
  exit 1
}
# Initializes JAVA_VERSION to the version of the JVM in use.
function init_java {
  if [ -z "$JAVA_HOME" ]; then
    error "JAVA_HOME is not set."
  fi
  JAVA_VERSION=$("${JAVA_HOME}"/bin/javac -version 2>&1 | cut -d " " -f 2)
  export JAVA_VERSION
}

# Initializes MVN_EXTRA_OPTS and SBT_OPTS depending on the JAVA_VERSION in use. Requires init_java.
function init_maven_sbt {
  MVN="build/mvn -B"
  MVN_EXTRA_OPTS=
  SBT_OPTS=
  if [[ $JAVA_VERSION < "1.8." ]]; then
    # Needed for maven central when using Java 7.
    SBT_OPTS="-Dhttps.protocols=TLSv1.1,TLSv1.2"
    MVN_EXTRA_OPTS="-Dhttps.protocols=TLSv1.1,TLSv1.2"
    MVN="$MVN $MVN_EXTRA_OPTS"
  fi
  export MVN MVN_EXTRA_OPTS SBT_OPTS
}
PUBLISH_SCALA_2_11=1
PUBLISH_SCALA_2_12=1
init_java
init_maven_sbt
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
# Commit ref to checkout when building
GIT_REF=${GIT_REF:-master}

if [ -z "$SPARK_VERSION" ]; then
  # Run $MVN in a separate command so that 'set -e' does the right thing.
  TMP=$(mktemp)
  $MVN help:evaluate -Dexpression=project.version > $TMP
  SPARK_VERSION=$(cat $TMP | grep -v INFO | grep -v WARNING | grep -v Download)
  rm $TMP
fi

tmp_repo=$(mktemp -d spark-repo-XXXXX)

# Hive-specific profiles for some builds
HIVE_PROFILES="-Phive"
# Profiles for publishing snapshots and release to Maven Central
PUBLISH_PROFILES="$BASE_PROFILES $HIVE_PROFILES"
# Profiles for building binary releases
BASE_RELEASE_PROFILES="$BASE_PROFILES -Psparkr"

# Generate random point for Zinc
export ZINC_PORT=$(python -S -c "import random; print random.randrange(3030,4030)")
SCALA_2_11_PROFILES=
if [[ $SPARK_VERSION > "2.3" ]]; then
  BASE_PROFILES="$-Pkubernetes -Pflume"
  SCALA_2_11_PROFILES="-Pkafka-0-8"
fi
echo "Publishing Spark checkout at '$GIT_REF' ($git_hash)"
echo "Publish version is $SPARK_VERSION"
# Coerce the requested version
$MVN versions:set -DnewVersion=$SPARK_VERSION
PROFILES="-Phadoop-2.7 -Pkubernetes -Phive"

if [[ $PUBLISH_SCALA_2_11 = 1 ]]; then
  $MVN -DzincPort=$ZINC_PORT -Dmaven.repo.local=$tmp_repo $SCALA_2_11_PROFILES $PROFILES \
  -DskipTests clean install
fi

if [[ $PUBLISH_SCALA_2_12 = 1 ]]; then
  ./dev/change-scala-version.sh 2.12
  $MVN -DzincPort=$((ZINC_PORT + 2)) $PROFILES -Dmaven.repo.local=$tmp_repo -Pscala-2.12 \
  -DskipTests clean install
fi

./dev/change-scala-version.sh 2.11

pushd $tmp_repo/org/apache/spark

# Remove any extra files generated during install
find . -type f |grep -v \.jar |grep -v \.pom | xargs rm

echo "Creating hash and signature files"
# this must have .asc, .md5 and .sha1 - it really doesn't like anything else there
for file in $(find . -type f)
do
  # echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --output $file.asc \
  #   --detach-sig --armour $file;
  if [ $(command -v md5) ]; then
    # Available on OS X; -q to keep only hash
    md5 -q $file > $file.md5
  else
    # Available on Linux; cut to keep only hash
    md5sum $file | cut -f1 -d' ' > $file.md5
  fi
  sha1sum $file | cut -f1 -d' ' > $file.sha1
done

bintray_upload=https://api.bintray.com/content/lightbend/commercial-releases/fdp-spark/2.1.2
echo "Uplading files to $bintray_upload"
for file in $(find . -type f)
do
  # strip leading ./
  echo "Source file $file to upload"
  file_short=$(echo $file | sed -e "s/\.\///")
  echo $file_short
  dest_url="$bintray_upload/org/apache/spark/$file_short"
  echo "Uploading $file_short"
  curl -T $file -u$BINTRAY_USER:$BINTRAY_API_KEY $dest_url
done
popd
