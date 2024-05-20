vim tests/integration-tests/src/test/java/org/apache/activemq/artemis/tests/integration/amqp/ErwinTest.java

set -e
#cd activemq-artemis
#mvn install -DskipTests --fae
#(cd artemis-hawtio/activemq-branding; mvn clean install -DskipTests --fae)
#(cd artemis-hawtio/artemis-plugin; mvn clean install -DskipTests --fae)
#(cd artemis-distribution; mvn clean install -DskipTests --fae)

#(cd tests/integration-tests; mvn -P jdk11to15-errorprone -DskipIntegrationTests=false -Dtest="ErwinTest#testCase3c" test 2>&1)
(cd tests/integration-tests; mvn -P jdk11to15-errorprone -DskipIntegrationTests=false -Dtest="ErwinTest" test 2>&1)
