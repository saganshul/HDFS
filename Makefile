.PHONY : all

all : Client NameNode
	make -C Client
	make -C NameNode

clean :
	make -C Client clean
	make -C NameNode clean

Client :
	make -C Client

NameNode :
	make -C NameNode

%.class : %.java
	javac -cp .:/usr/share/java/protobuf.jar $^
