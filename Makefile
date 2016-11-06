.PHONY : all

all : ProtoBuf Client NameNode DataNode
	make -C ProtoBuf
	make -C Client
	make -C NameNode
	make -C DataNode

clean :
	make -C ProtoBuf clean
	make -C Client clean
	make -C NameNode clean
	make -C DataNode clean

Client :
	make -C Client

NameNode :
	make -C NameNode

ProtoBuf :
	make -C ProtoBuf

DataNode :
	make -C DataNode

%.class : %.java
	javac -cp .:/usr/share/java/protobuf.jar $^
