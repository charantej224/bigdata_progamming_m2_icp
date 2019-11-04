import java.io._

object WriteFiles {

  val directory = "/log/file"
  val text = "Write without non repeating word to see if its rightly counted\n"

  def main(args: Array[String]): Unit = {
    for (w <- 0 to 40) {
      val file = new File(directory+w)
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(text)
      bw.close()
      Thread.sleep(10 * 1000) // wait for 10 seconds.
    }
  }

}
