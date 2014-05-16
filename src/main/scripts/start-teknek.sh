CP=`ls teknek-core*.jar`
for f in `ls lib/*` ; do
  CP=$CP:$f
done
java -Dlog4j.configuration=log4j.properties -Dteknek.zk.servers=localhost:2181 -cp $CP:. io.teknek.daemon.TeknekDaemon
