.PHONY : all

all : ProtoBuf Client NameNode
	make -C ProtoBuf
	make -C Client
	make -C NameNode

clean :
	make -C ProtoBuf clean
	make -C Client clean
	make -C NameNode clean

Client :
	make -C Client

NameNode :
	make -C NameNode

ProtoBuf :
	make -C ProtoBuf

%.class : %.java
	javac -cp .:/usr/share/java/protobuf.jar $^
