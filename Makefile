all: Server.class UserNode.class MessageBody.class Commit.class ServerStateRestorer.class

%.class: %.java
	javac $<

clean:
	rm -f *.class
