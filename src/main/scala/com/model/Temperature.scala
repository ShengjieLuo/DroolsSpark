package com.model

class Temperature {
  private var value:Double = -273.99
  private var status:Int = 0
 
  def setValue(value:Double){ this.value = value}
  def getValue = this.value

  def setStatus(status:Int) { this.status = status}
  def getStatus = this.status

  def this(value:Double){this();this.value=value;this.status=0;}
}
