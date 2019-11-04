package org.nationwidechildrens.igm.vienna

import scala.collection.mutable.ArrayBuffer
import java.util.Calendar

// TODO: Document
class FastaData(var transcriptId: String) extends java.io.Serializable {
  
  var items = ArrayBuffer[BaseCombo]()
  
  override def toString():String = {
    
    var str = new StringBuilder()
    
    items.foreach { f =>
       str.append(this.transcriptId)
       str.append("|")
       str.append(f.position)
       str.append("|")
       str.append(f.base)
       str.append("|")
       str.append(f.sequence)
       str.append("|")
       str.append("#")
       
       str.append(f.alternates(0)._1)
       str.append("|")
       str.append(f.alternates(0)._2)
       str.append("|")
       str.append(f.alternates(0)._3)
       str.append("|")
       
       str.append(f.alternates(1)._1)
       str.append("|")
       str.append(f.alternates(1)._2)
       str.append("|")
       str.append(f.alternates(1)._3)
       str.append("|")
       
       str.append(f.alternates(2)._1)
       str.append("|")
       str.append(f.alternates(2)._2)
       str.append("|")
       str.append(f.alternates(2)._3)
       str.append("|")       
       
       str.append("\n")
    }
        
    return str.toString()
  }
  
  def toTupleList() = {
    items.map( item => {
      List(
           (this.transcriptId, item.base, item.alternates(0)._1, item.alternates(0)._2, item.alternates(0)._3, item.sequence),
           (this.transcriptId, item.base, item.alternates(1)._1, item.alternates(1)._2, item.alternates(1)._3, item.sequence),
           (this.transcriptId, item.base, item.alternates(2)._1, item.alternates(2)._2, item.alternates(2)._3, item.sequence)
          )
    })
  }
}


