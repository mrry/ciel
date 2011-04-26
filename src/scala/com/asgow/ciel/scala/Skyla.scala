import com.asgow.ciel.executor._
import com.asgow.ciel.rpc._
import com.asgow.ciel.tasks._
import com.asgow.ciel.references._
import scala.util.continuations._

package com.asgow.ciel.scala {

  class FunctionTask[T](func : Unit => T) extends SingleOutputTask[T] {
    
    override def run : T = {
      func()
    }

  }

  case class BlockException(cont : Unit => Unit, ref : Reference) extends Throwable

  class SkylaFuture[T](fut : CielFuture[T]) extends FutureReference(fut.getId()) {

    def get : T @suspendable = {
      try {
	val filename = Ciel.RPC.getFilenameForReference(this)
	val fis = new java.io.FileInputStream(filename)
	val ois = new java.io.ObjectInputStream(fis)
	ois.readObject().asInstanceOf[T]
      } catch {
        case rue: ReferenceUnavailableException => {
	  shift { (cont : Unit => Unit) =>
	    throw new BlockException(cont, this)
          }
	  get
        }
      }
    }

  }

  object Skyla {

    def spawn[T](func : Unit => T) : SkylaFuture[T] = {
      val task = new FunctionTask(func)
      new SkylaFuture(com.asgow.ciel.executor.Ciel.spawnSingle(task))
    }

  }

  abstract class SkylaThread[T] extends FirstClassJavaTask {
  
    override def setup = { }

    override def invoke = { 
      try {
	reset {
	  val result = run
	  Ciel.returnObject(result)
        }
      } catch {
	case be: BlockException => {
	  val contTask = new SkylaContinuation(be.cont, Array(be.ref))
	  Ciel.tailSpawn(contTask, null)
        }
      }
    }

    override def getDependencies = Array()

    def run : T @suspendable

  }

  class SkylaContinuation[T](cont : Unit => Unit, deps : Array[Reference]) extends FirstClassJavaTask {

    override def setup = { }

    override def invoke = {
      try {
	cont()
      } catch {
	case be: BlockException => {
	  val contTask = new SkylaContinuation(be.cont, Array(be.ref))
	  Ciel.tailSpawn(contTask, null)
        }
      }
    }

    override def getDependencies = deps

  }

}
