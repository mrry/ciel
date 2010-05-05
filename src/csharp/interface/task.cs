using System;
using System.IO;

namespace uk.co.mrry.mercator.task {

  public interface Task {

    void invoke(Stream[] file_inputs, Stream[] file_outputs, String[] argv);

  }

}