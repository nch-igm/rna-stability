package org.nationwidechildrens.igm.vienna

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import java.io._

/*
This is a model class to hold data from a row in a GFF file. I pulled the file column definitions from:

https://en.wikipedia.org/wiki/General_feature_format 

... and have included them below. 
 
[Column][Name][Description]
1	sequence	The name of the sequence where the feature is located.
2	source	Keyword identifying the source of the feature, like a program (e.g. Augustus or RepeatMasker) or an organization (like TAIR).
3	feature	The feature type name, like "gene" or "exon". In a well structured GFF file, all the children features always follow their parents in a single block (so all exons of a transcript are put after their parent "transcript" feature line and before any other parent transcript line). In GFF3, all features and their relationships should be compatible with the standards released by the Sequence Ontology Project.
4	start	Genomic start of the feature, with a 1-base offset. This is in contrast with other 0-offset half-open sequence formats, like BED files.
5	end	Genomic end of the feature, with a 1-base offset. This is the same end coordinate as it is in 0-offset half-open sequence formats, like BED files.[citation needed]
6	score	Numeric value that generally indicates the confidence of the source on the annotated feature. A value of "." (a dot) is used to define a null value.
7	strand	Single character that indicates the Sense (molecular biology) strand of the feature; it can assume the values of "+" (positive, or 5'->3'), "-", (negative, or 3'->5'), "." (undetermined).
8	frame (GTF, GFF2) or phase (GFF3)	Frame or phase of CDS features; it can be either one of 0, 1, 2 (for CDS features) or "." (for everything else). Frame and Phase are not the same, See following subsection.
9	Attributes.	All the other information pertaining to this feature. The format, structure and content of this field is the one which varies the most between the three competing file formats.
 */

@SerialVersionUID(123L)
class GffData extends Serializable {
  var sequence = ""
  var genomicStart = 0
  var genomicEnd = 0
  var score = 0.0
  var difference = 0
  var transcriptId = ""
  var objList = ListBuffer[GffData]()
  var transcriptStartPosition = 0
  var transcriptEndPosition = 0
  var chromosomeNumber = 0
  var sense = ""
  var offset = 0
  var hasGap = false
  var gapQueue = new Queue[String]
  var cachedPositionLookup: Map[Int, Int] = null  
  
  // This constructor takes a line like this:
  // NC_000002.12    RefSeq  cDNA_match      178807212       178807423       212     -       .       ID=70a3df56-8945-4f6f-acd2-d664ad7131ce;Target=NM_001267550.2 1 212 +;gap_count=0;identity=1;idty=1;num_ident=109224;num_mismatch=0;pct_coverage=100;pct_identity_gap=100;pct_identity_ungap=100
  // and parses it based on the rows described above.
  def this(gffLine: String) = {
    this()
    
    if (gffLine != null) {
      val words = gffLine.split("\\s+")
      
      this.sequence = words(0)
      this.genomicStart = words(3).toInt
      this.genomicEnd = words(4).toInt
      this.score = words(5).toDouble
      this.sense = words(6)
    
      // The last column has all manners of crazy data, but in the case of Vienna we just two to split on
      // the = sign so we can get the transcriptId  
      val subWords = words(8).split("=")
      this.transcriptId = subWords(2)
      
      this.transcriptStartPosition = words(9).toInt  // In the example above this is 1
      this.transcriptEndPosition = words(10).toInt // In the example above this is 212
      
      // Pull the chromosome number out of the sequence using this regex
      val pattern = "([0-9]*?\\.)".r
      chromosomeNumber = pattern.findAllIn(sequence).toList(0).dropRight(1).toInt
      
      // For our value to go along with our key we need to calculate the differences between the numbers in columns 4 and 5 in the file.
      // We then tack on one to cover the offset. (Remember CS 101, fence posts vs fence lengths)      
      this.difference = genomicEnd - genomicStart + 1
      
      var offset = 0
      
      val gapWords = gffLine.split(';')
      
      // We need to determine if our entry is a GFF row that indicates gaps in the mapping. Naturally
      // it is a little convoluted as the gap_count field will be > 0 for ANY GFF row for a transcript,
      // not just the one that actually has the gap. Example:
      //
      // gap_count=1
      // 
      // So, base our first if test on that...
      if (gapWords(2).split('=')(1).toInt > 0) {
        val gapTag = gapWords.last.split('=')
        
        // And then the second on if there is a Gap tag like this:
        // Gap=M333 D3 M123
        if (gapTag(0) == "Gap") {
          // If there is a Gap=, then split apart its parts and calculate the offset
          val gapItems = gapTag(1).split(' ')
                  
          gapItems.foreach { item =>
            val num = item.take(1) match {
              case "I" => item.substring(1, item.length).toInt
              case "D" => (0 - item.substring(1, item.length).toInt)
              case _ => 0
            }
            
            // And most importantly add the gap item to the queue for later processing.
            this.gapQueue += item
            this.offset += num
          }
          
          // Hey look, sense vs antisense again. Thus far we added the items to the queue
          // assuming sense. If it is antisense however we need to reverse it to make things
          // match up.
          if (this.sense == "-") {
            this.gapQueue = this.gapQueue.reverse
          }
          
          this.hasGap = true          
        }     
      }      
    }       
  }
  
  /**
   * Method that builds a lookup table of transcript positions to genomic positions. For the entire
   * data set this method is WAY to memory intensive, but for GFF records with gaps indicated it is 
   * the best way of getting the correct answer.
   */
  def buildPositionLookup(transcriptPosition: Int): Map[Int,Int] = {
    // If we have already built the lookup bail out early, no sense in doing it again.
    if (this.cachedPositionLookup != null) {
      return this.cachedPositionLookup
    }
    
      var positionLookup = scala.collection.mutable.Map[Int, Int]()              
      var gapType:String = null
      var gapLength = -1
      var gapOffset = 0
      
      var genomicIndex = genomicStart
            
      // We need to get our copy of the queue of gap indicators. Because this is a singleton object
      // if we started monkeying with the gapQueue member it could impact other processing.
      val localGapQueue = this.gapQueue.clone()      
      
      // Default by going in the sense direction.
      var startPos = this.transcriptStartPosition
      var endPos = this.transcriptEndPosition
      var stepBy = 1
      
      // But if we are antisense, flip everything around.
      if (this.isAntiSense()) {
        startPos = this.transcriptEndPosition
        endPos = this.transcriptStartPosition
        stepBy = -1
      }
      
      // Walk from the start transcript position to the end transcript position and calculate
      // the genomic position for each one, taking into consideration the gaps.
      for (i <- startPos to endPos by stepBy) {
        if (!this.hasGap) {
          // Note: We should only get called here if change the calling code to not just do 
          // GFF records that gaps in them. If we do that though, TONS of memory will get used.
          positionLookup += ( i -> genomicIndex )
        } else {         
          // The basic algorithm is we pull a gap definition off the queue and then use that to
          // determine if we need to adjust the offset. For example, let's imagine we have this
          // three gap item queue:
          //	M5 I1 M7
          // In this case, for the first five iterations through the loop, because we match the
          // first five alleles (M5) we are going to do nothing to the offset. The next iteration
          // we pull the next gap item (I1) and thus add one to the offset since a nucelotide has 
          // been inserted. We pull the final gap item (M7, for match seven) and walk the rest of the
          // way. Note that after we add one to the offset because of I1, we are adding that same
          // offset to each position after it.
          //
          // Similarly, if we had a delete like below (D1) instead of an insert, we would subtract
          // one from the offset.
          // M501 D1 M409 
          if (gapType == null) {
            val queueItem = localGapQueue.dequeue()
            
            gapType = queueItem.take(1)
            gapLength = queueItem.substring(1, queueItem.length).toInt
            
            if (gapType == "I") {
              gapOffset -= gapLength
            } else if (gapType == "D") {
              gapOffset += gapLength
            } // an "M" would just add 0 so do nothing...                         
          }
          
          // Add our calculated genomic position to the lookup map with the transcript position as the key.
          positionLookup += ( i -> (genomicIndex + gapOffset))

          // Take one away from the current gap item. For example, if we had just popped of M5, now we are M4
          gapLength -= 1
          
          // If we have exhausted the gap item null it out so we pull a new one off the queue next iteration.
          if (gapLength == 0) {
            gapType = null
          }
          
        }

        // Increment the genomicIndex to keep in step with the transcript index.
        genomicIndex += 1
      }    
                 
      // Finally cache our lookup and return it to be used
      this.cachedPositionLookup = positionLookup.toMap
      
      this.cachedPositionLookup
  }
  
  def isSense(): Boolean = {
    (this.sense == "+")
  }
  
  def isAntiSense(): Boolean = {
    (this.sense == "-")    
  }
  
  def getSense(): String = {
    this.sense
  }
  
  /**
   * Part of the process of mapping the genomic position is also making sure we have the correct
   * nucelotide value. This can be the exact opposite of what is in the .rna file if it is an
   * antisense strand. If we are antisense go ahead and flip the nucelotide to be correct.
   */
  def reverseComplimentNucleotide(nucleotide: String): String = {
    var newValue = nucleotide
    
    if (this.isAntiSense()) {
      newValue = nucleotide match {
        case "A" => "T"
        case "C" => "G"
        case "G" => "C"
        case "T" => "A"
        case "N" => "N"
      }
    }
    
    newValue
  }
  
  // The easiest way I could think of to handle the reduceByKey call was to create an internal array of nested
  // GffData objects. This is useful when a transcriptId has more than one line in the GFF file.
  def add(data: GffData): GffData = {
    this.objList += data
    
    return this
  }
  
  // In order for the reduceByKey at the end of a map to work we need to have a key. 
  // To do that we we create one in this format:
  // NC_000006.12|NM_001129895.2  
  def getKey() : String = {
    return sequence + "|" + transcriptId
  }
  

  // Method that gets the total difference of all GffData objects for this transcript. Walk the array that
  // has been populated too, not just this top level GffData object.
  def getTotalDifference() : Int = {    
    var diff = this.difference

    this.objList.foreach { obj => 
      diff += obj.difference
    }   
       
    return diff
  }
  
  // Method that sees if this object contains the range that the given transcriptPosition resides in.
  def containsTranscriptPosition(transcriptPosition: Int) : Boolean = {
    return (transcriptPosition >= this.transcriptStartPosition && transcriptPosition <= transcriptEndPosition)
  }
  
  // Here we are trading processing power to relieve some massive memory constraints. For the given transcript position
  // figure out what the chromosome position is for it.
  def getChromosomePosition(transcriptPosition: Int) : Int = {        
    var chromosomePosition = -1
           
    // Note in the calculations below we need to determine the genomic coordinate differently for a sense vs antisense strand. 
    // This is because we need to count forward with sense and backwards with antisense.
    if (this.containsTranscriptPosition(transcriptPosition)) {
      // This transcript position is in this object, lucky us. Do the calculation to find the genomic position.
      if (this.isSense()) {
        
    	// If we are dealing with a entry from the GFF file that indicates we have a gap we need to
    	// process it with the more memory intensive lookup method. Otherwise we can just do the 
    	// calculation for non-gap records and save the memory.
        if (this.hasGap) {
          val positionLookup = this.buildPositionLookup(transcriptPosition)        
          chromosomePosition = positionLookup.getOrElse(transcriptPosition, -1)         
        } else {
          chromosomePosition = this.genomicStart + (this.difference - (this.transcriptEndPosition - transcriptPosition)) -1 + this.offset
        }
      } else if (this.isAntiSense()) {        
    	// Everything is backwards if the record is indicated as anti sense thus do the processing
    	// a little different. (Actually the gap processing is the same, this could really use a refactor.
        if (this.hasGap) {
          val positionLookup = this.buildPositionLookup(transcriptPosition)        
          chromosomePosition = positionLookup.getOrElse(transcriptPosition, -1)                         
        } else {        
          chromosomePosition = this.genomicEnd - (this.difference - (this.transcriptEndPosition - transcriptPosition)) + 1 + this.offset
        }
      }
    } else {
      // Walk the list looking for it
      this.objList.foreach { obj => 
        if (obj.containsTranscriptPosition(transcriptPosition)) {                                        
          // Found it on one of our children. Do the calculation to find the genomic position.
          if (obj.isSense()) {
            if (obj.hasGap) {
              val positionLookup = obj.buildPositionLookup(transcriptPosition)
              chromosomePosition = positionLookup.getOrElse(transcriptPosition, -1)
            } else {
              chromosomePosition = obj.genomicStart + (obj.difference - (obj.transcriptEndPosition - transcriptPosition)) - 1 + obj.offset
            }            
          } else if (obj.isAntiSense()) {
            if (obj.hasGap) {
              val positionLookup = obj.buildPositionLookup(transcriptPosition)
              chromosomePosition = positionLookup.getOrElse(transcriptPosition, -1)
            } else {                       
              chromosomePosition = obj.genomicEnd - (obj.difference - (obj.transcriptEndPosition - transcriptPosition)) + 1 + obj.offset
            }
          }                    
        }
      }
      
    }
    
    if (chromosomePosition == -1) {
      println("$$$$$$$$$$$$$ Couldn't find transcriptId[transcriptPosition]: " + transcriptId + "[" + transcriptPosition + "]")
    }
    
    return chromosomePosition
  }
  
  def getChromosomeNumber() : String = {
    var returnVal = ""
    
    if (chromosomeNumber == 23) {
      returnVal = "X"
    } else if (chromosomeNumber == 24) {
      returnVal = "Y"
    } else {
      returnVal = chromosomeNumber.toString()
    }
    
    return returnVal
  }
}