package com.asgow.ciel.scala {

  class FunctionTask[T](func : Unit => T) extends com.asgow.ciel.tasks.SingleOutputTask[T] {
    
    override def run : T = {
      func()
    }

  }

  object Skyla {

    def spawn[T](func : Unit => T) : com.asgow.ciel.references.CielFuture[T] = {
      val task = new FunctionTask(func)
      com.asgow.ciel.executor.Ciel.spawnSingle(task)
    }

  }

}
