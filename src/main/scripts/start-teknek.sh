CP=`ls teknek-core*.jar`
for f in `ls lib/*` ; do
  CP=$CP:$f
done
java -Dteknek.zk.servers=localhost:2181 -cp $CP io.teknek.daemon.TeknekDaemon

