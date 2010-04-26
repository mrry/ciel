all: hello_master.dll

hello_master.dll: hello_master.cs different_location/hello_slave.dll ../interface/task.dll
	mcs -t:library -r:../interface/task.dll -r:different_location/hello_slave.dll hello_master.cs

different_location/hello_slave.dll: different_location/hello_slave.cs
	cd different_location; 	mcs -t:library hello_slave.cs; cd ..
