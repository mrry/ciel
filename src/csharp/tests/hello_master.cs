
using System;
using System.IO;
using ciel;

public class HelloMaster : Task
{
  public void invoke(Stream[] inputs, Stream[] outputs, String[] args)
  {
    HelloSlave slave = new HelloSlave();
    Console.WriteLine("Calling slave...");
    slave.f();
    Console.WriteLine("Call returned");

    Console.WriteLine("I have {0} inputs, {1} outputs and {2} arguments",
		      inputs.Length, outputs.Length, args.Length);

    int i = 0;
    foreach(Stream s in inputs) {

      Console.WriteLine("Input {0} begins with {1}", i++, (char)s.ReadByte());

    }
    i = 0;
    foreach(Stream t in outputs) {

      Console.WriteLine("Writing {0} to output {1}", (char)(i + (int)'a'), i);
      t.WriteByte((byte)(i + (int)'a'));
      i++;

    }

    i = 0;
    foreach(String u in args) {

      Console.WriteLine("Argument {0} is {1}", i++, u);

    }

  }
  
}