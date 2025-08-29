export JAVA_HOME="/opt/homebrew/opt/openjdk@21/"

# use --master local[*] for all cores
$(brew --prefix apache-spark)/libexec/sbin/start-thriftserver.sh \
  --master local[1] \
  --driver-memory 16g \
  --executor-memory 16g \
  --conf spark.sql.catalogImplementation=hive

# $(brew --prefix apache-spark)/libexec/sbin/stop-thriftserver.sh
