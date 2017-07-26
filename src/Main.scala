object Main {
  val usage =
    """
    Usage: matrix [--slaves num] --mode (master|slave) <file>
  """

  def main(args: Array[String]) {
    val options = parse(args)
    println(options)
  }

  def parse(args: Array[String]): Unit = {
    if (args.length == 0) {
      println(usage)
    }
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--slaves" :: value :: tail =>
          nextOption(map ++ Map('slaves -> value.toInt), tail)
        case "--mode" :: value :: tail =>
          nextOption(map ++ Map('slaves -> value), tail)
        case string :: Nil => nextOption(map ++ Map('file -> string), list.tail)
        case option :: tail => println("Unknown option " + option)
          throw new IllegalArgumentException()
      }
    }

    return nextOption(Map(), arglist)
  }
}