import com.asgow.ciel.executor._
import com.asgow.ciel.rpc._
import com.asgow.ciel.tasks._
import com.asgow.ciel.references._
import scala.collection._
import scala.util.continuations._

package com.asgow.ciel.scala {

  class FunctionTask[T](func : Unit => T) extends SingleOutputTask[T] {
    
    override def run : T = {
      func()
    }

  }

  class GeneratorYielder[T](stream : java.io.ObjectOutputStream) {

    def yieldFunc(obj : T) : Unit = {
      stream.writeObject(obj)
    }

  }

  class GeneratorTask[T](func : (T => Unit) => Unit) extends FirstClassJavaTask {

    override def setup : Unit = { }

    override def invoke : Unit = {
      val out = Ciel.RPC.getOutputFilename(0)
      val oos = new java.io.ObjectOutputStream(out.open())
      func(new GeneratorYielder(oos).yieldFunc)
      oos.close
    }

    override def getDependencies = Array()

  }

  class ChainedGeneratorTask[T, U](func : (T => Unit) => Unit, parent : SkylaGenerator[U]) extends GeneratorTask[T](func) {

    override def getDependencies = Array(parent)

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

  class GeneratorIterator[T](stream : java.io.ObjectInputStream) extends Iterator[T] {

    private var done = false
    private var nextElement : Option[T] = None

    override def hasNext() : Boolean = {
      if (done) {
        false
      } else {
        try {
          nextElement = Some(stream.readObject().asInstanceOf[T])
          true
        } catch {
          case eofe: java.io.EOFException => {
            done = true
            nextElement = None
            false
          }
        }
      }
    }

    override def next() : T = nextElement match {
      case None => {
        hasNext
        next
      }
      case Some(value) => {
        nextElement = None
        value
      }
    }

  }

  class SkylaGenerator[T](fut : Reference) extends FutureReference(fut.getId()) {
    
    def iterator : Iterator[T] @suspendable = {
      try {
        val filename = Ciel.RPC.getFilenameForReference(this)
	val fis = new java.io.FileInputStream(filename)
        val ois = new java.io.ObjectInputStream(fis)
        new GeneratorIterator[T](ois)
      } catch {
        case rue: ReferenceUnavailableException => {
          shift { (cont : Unit => Unit) =>
            throw new BlockException(cont, this)
          }
          iterator
        }
      }

    }

  }

  object Skyla {

    def spawn[T](func : Unit => T) : SkylaFuture[T] = {
      val task = new FunctionTask(func)
      new SkylaFuture(com.asgow.ciel.executor.Ciel.spawnSingle(task))
    }

    def spawnGenerator[T](func : (T => Unit) => Unit) : SkylaGenerator[T] = {
      val task = new GeneratorTask[T](func)
      new SkylaGenerator(com.asgow.ciel.executor.Ciel.spawn(task, Array[String](), 1)(0))
    }

    def spawnChainedGenerator[T, U](func : (T => Unit) => Unit, parent : SkylaGenerator[U]) : SkylaGenerator[T] = {
      val task = new ChainedGeneratorTask(func, parent)
      new SkylaGenerator(com.asgow.ciel.executor.Ciel.spawn(task, Array[String](), 1)(0))
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
