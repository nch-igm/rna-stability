package org.nationwidechildrens.igm.vienna

class BaseCombo(var position: Int, var base: Char, var sequence: String) extends java.io.Serializable {
  
  // There is only three possible alternates so who cares the order of the list
  var alternates = List[(Int,Char,String)]()  
       
  def addAlternateCombo(position: Int, base: Char, sequence: String) = {    
    alternates = (position, base, sequence) :: alternates
  }    
}