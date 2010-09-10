
all: JitteryProducer.jar JitteryConsumer.jar

JitteryProducer.jar: JitteryProducer.java
	javac -cp .. JitteryProducer.java
	jar cf JitteryProducer.jar JitteryProducer.class StreamFeeder.class

JitteryConsumer.jar: JitteryConsumer.java
	javac -cp .. JitteryConsumer.java
	jar cf JitteryConsumer.jar JitteryConsumer.class StreamConsumer.class