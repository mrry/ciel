
all: JitteryProducer.jar JitteryConsumer.jar

JitteryProducer.jar: tests/JitteryProducer.java
	jar cf JitteryProducer.jar tests/JitteryProducer.class tests/StreamFeeder.class

JitteryConsumer.jar: tests/JitteryConsumer.java
	jar cf JitteryConsumer.jar tests/JitteryConsumer.class tests/StreamConsumer.class