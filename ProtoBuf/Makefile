all : HDFSProtoBuf.class

%.class : %.java
	 javac -classpath ..:/usr/share/java/protobuf.jar $^

HDFSProtoBuf.java : HDFS.proto
	protoc --java_out=.. HDFS.proto

clean:
	rm -f *.class  HDFSProtoBuf.java
