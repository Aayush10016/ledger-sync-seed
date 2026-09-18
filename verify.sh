#!/usr/bin/env bash
# Compiles and runs the pipeline against fixtures/corpus-a.jsonl.
# Needs a JDK 21 and nothing else - no network, no database, no Gradle.
set -euo pipefail
cd "$(dirname "$0")"

echo "==> compiling"
rm -rf build/selfcheck && mkdir -p build/selfcheck
javac -d build/selfcheck $(find src/main/java -name '*.java' ! -name 'DynamoDbLedgerStore.java' ! -name 'DynamoDbLedgerAdapter.java')

echo
echo "==> running"
set +e
java -cp build/selfcheck in.simplifymoney.ledgersync.SelfCheck "$@"
EXIT_CODE=$?
set -e
if [ $EXIT_CODE -ne 0 ]; then
  echo "Verification failed due to discrepancies."
  exit 1
fi

echo "Verification passed."
exit 0
