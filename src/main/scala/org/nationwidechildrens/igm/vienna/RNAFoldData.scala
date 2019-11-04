package org.nationwidechildrens.igm.vienna

class RNAFoldData() {
  var transcript_id = ""
  var transcript_position = 0
  var ref = ""
  var alt = ""
  var wildSequence = ""
  var isWildType = false

  var sequence = ""
  var mfeValue = 0.0
  var mfeStructure = ""
  var efeValue = 0.0
  var efeStructure = ""
  var meafeValue = 0.0
  var meaValue = 0.0
  var meafeStructure = ""
  var cfeValue = 0.0
  var cfeStructure = ""
  var freqMfeEnsemble = 0.0
  var endValue = 0.0
  var cdValue = 0.0
  
  var deltaMfeValue = 0.0
  var deltaEfeValue = 0.0
  var deltaMeafeValue = 0.0
  var deltaCfeValue = 0.0
  var deltaEndValue = 0.0
  var deltaCdValue = 0.0

  var mfeed = 0
  var meaed = 0
  var cfeed = 0
  
  
  def calcuateDeltasFromWild(wildRNAFoldData: RNAFoldData) = {
    this.deltaMfeValue = this.subtraceWildFromAlt(wildRNAFoldData.mfeValue, mfeValue) 
    this.deltaEfeValue = this.subtraceWildFromAlt(wildRNAFoldData.efeValue, efeValue) 
    this.deltaMeafeValue = this.subtraceWildFromAlt(wildRNAFoldData.meafeValue, meafeValue)  
    this.deltaCfeValue = this.subtraceWildFromAlt(wildRNAFoldData.cfeValue, cfeValue)  
    this.deltaEndValue = this.subtraceWildFromAlt(wildRNAFoldData.endValue, endValue) 
    this.deltaCdValue = this.subtraceWildFromAlt(wildRNAFoldData.cdValue, cdValue)  
  }
  
  def subtraceWildFromAlt(wild: Double, alt: Double): Double = {    
    return (BigDecimal(alt).setScale(2, BigDecimal.RoundingMode.HALF_UP) - BigDecimal(wild).setScale(2, BigDecimal.RoundingMode.HALF_UP)).toDouble 
  }
}