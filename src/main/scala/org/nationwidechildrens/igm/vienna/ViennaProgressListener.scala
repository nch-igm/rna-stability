package org.nationwidechildrens.igm.vienna

import org.apache.spark.SparkConf
import org.apache.spark.SparkFirehoseListener
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.LongAccumulator

class ViennaProgressListener(conf: SparkConf, counter: LongAccumulator) extends SparkFirehoseListener {
  
  var lastCounterValue:Long = 0
  
  override def onEvent(event: SparkListenerEvent): Unit = {
    val counterValue = counter.value    
    
    if (counterValue > (lastCounterValue + 1000)) {
      lastCounterValue = counterValue
      
      println(lastCounterValue + " records have been processed")
    }
    
  }
}