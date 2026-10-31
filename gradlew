#!/bin/sh

# Attempt to set APP_HOME
# Resolve links: /usr/bin/bash may be a link
PRG="/usr/bin/bash"
# Need this for relative symlinks.
while [ -h "" ] ; do
    ls=
    link=
    if expr "" : '/.*' > /dev/null; then
        PRG=""
    else
        PRG=."/"
    fi
done
SAVED="/working_dir/c_4d1626fd8832d5cd"
cd "." >/dev/null
APP_HOME="/working_dir/c_4d1626fd8832d5cd"
cd "" >/dev/null

APP_NAME="Gradle"
APP_BASE_NAME=bash

# Add default JVM options here. You can also use JAVA_OPTS and GRADLE_OPTS to pass JVM options to this script.
DEFAULT_JVM_OPTS='"-Xmx64m" "-Xms64m"'

# Use the maximum available, or set MAX_FD != -1 to use that value.
MAX_FD="maximum"

warn () {
    echo ""
}

die () {
    echo
    echo ""
    echo
    exit 1
}

# OS specific support (must be 'true' or 'false').
cygwin=false
msys=false
darwin=false
nonstop=false
case "Linux" in
  CYGWIN* ) cygwin=true ;;
  Darwin* ) darwin=true ;;
  MINGW* ) msys=true ;;
  NONSTOP* ) nonstop=true ;;
esac

CLASSPATH=/gradle/wrapper/gradle-wrapper.jar

# Determine the Java command to use to start the JVM.
if [ -n "" ] ; then
    if [ -x "/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVACMD="/jre/sh/java"
    else
        JAVACMD="/bin/java"
    fi
    if [ ! -x "" ] ; then
        die "ERROR: JAVA_HOME is set to an invalid directory: "
    fi
else
    JAVACMD="java"
    which java >/dev/null 2>&1 || die "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH."
fi

# If gradle command exists in PATH (e.g. from GitHub Actions setup-gradle), invoke it directly if wrapper jar is absent
if [ ! -f "" ] && which gradle >/dev/null 2>&1 ; then
    exec gradle ""
fi

exec "" "-Dorg.gradle.appname=" -classpath "" org.gradle.wrapper.GradleWrapperMain ""
