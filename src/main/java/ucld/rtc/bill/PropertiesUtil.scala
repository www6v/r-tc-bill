package ucld.rtc.bill

import java.util.Properties

object PropertiesUtil {

  def getProperties() :Properties = {
    val properties = new Properties()
    val reader = getClass.getResourceAsStream("/my.properties")
    properties.load(reader)
    properties
  }

  def getPropString(key : String) : String = {
    getProperties().getProperty(key)
  }

  def getPropInt(key : String) : Int = {
    getProperties().getProperty(key).toInt
  }

  def getPropBoolean(key : String) : Boolean = {
    getProperties().getProperty(key).toBoolean
  }
}


