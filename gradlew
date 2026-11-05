#!/bin/sh
# Zeph gradlew: gradle-wrapper.jar varsa onu çalıştırır; yoksa PATH'teki 'gradle' komutunu kullanır.
# (GitHub Actions iş akışı Gradle 8.7'yi kendisi kurar ve doğrudan 'gradle assembleDebug' çalıştırır.)
APP_HOME=$(cd "$(dirname "$0")" >/dev/null 2>&1 && pwd -P)
WRAPPER_JAR="$APP_HOME/gradle/wrapper/gradle-wrapper.jar"

if [ -f "$WRAPPER_JAR" ]; then
    if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
        JAVACMD="$JAVA_HOME/bin/java"
    else
        JAVACMD="java"
    fi
    exec "$JAVACMD" -Xmx64m -Xms64m -classpath "$WRAPPER_JAR" org.gradle.wrapper.GradleWrapperMain "$@"
fi

if command -v gradle >/dev/null 2>&1; then
    exec gradle "$@"
fi

echo "HATA: gradle/wrapper/gradle-wrapper.jar yok ve 'gradle' komutu bulunamadı." >&2
echo "Çözüm: Gradle 8.7 kurun veya 'gradle wrapper --gradle-version 8.7' çalıştırıp wrapper dosyalarını ekleyin." >&2
exit 1
