package com.github.mogproject.redismock.util


object StringUtil {
  /**
   * convert glob-style string to regex string
   * @param glob glob-style string
   * @return regex string (not Regex instance)
   */
  def globToRegex(glob: String): String = {
    val b = new StringBuilder
    glob.foreach {
      case c if  "\\/$^+.()=!|.,".contains(c) =>
        b += '\\'
        b += c
      case '?' =>
        b += '.'
      case '*' =>
        b ++= ".*"
      case c =>
        b += c
    }
    b.result()
  }
}
