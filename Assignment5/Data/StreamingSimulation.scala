import java.io.{PrintWriter}
import java.net.ServerSocket

object StreamingSimulation {

  def main(args: Array[String]) {

    // Three args: directory of file, port #, time interval(millisecond)

    if (args.length != 2) {

      System.err.println("Usage: <port> <millisecond>")

      System.exit(1)

    }

    val listener = new ServerSocket(args(0).toInt)

    while (true) {

      val socket = listener.accept()

      new Thread() {

        override def run = {

          println("Got client connected from: " + socket.getInetAddress)

          val out = new PrintWriter(socket.getOutputStream(), true)
          var content = "0"

          while (true) {
            content = (new util.Random).nextInt(2).toString
            Thread.sleep(args(1).toLong)
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }

      }.start()

    }

  }

}
