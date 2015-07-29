

java -server -Xms2g -Xmx2g -jar lsu-1.0-SNAPSHOT-jar-with-dependencies.jar redis-19453.onprem-pub.demo.redislabs.com 19453 /home/ubuntu/lsudata 55821043 100 true 1java -server -Xms2g -Xmx2g -jar lsu-1.0-SNAPSHOT-jar-with-dependencies.jar redis-19453.onprem-pub.demo.redislabs.com 19453 /home/ubuntu/lsudata 55821043 100 true 1
#nohup time java -server -Xms2g -Xmx2g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=60 -XX:+UseCMSInitiatingOccupancyOnly -XX:+ExplicitGCInvokesConcurrent -jar lsu-1.0-SNAPSHOT-jar-with-dependencies.jar 192.168.70.101 13409 /run/shm/metagenome 55821043 100 true 1 &
