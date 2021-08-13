fun main(args: Array<String>) {

    mainMenu()

}

private fun mainMenu() {
    println("please choose one : ")
    println("1- simple producer")
    println("2- simple consumer")
    println("3- word counter app")

    val input = readLine()

    if (input.isNullOrEmpty()) {
        mainMenu()
        return
    }

    if(!menuNumberIsValid(input, 1..3))
    {
        mainMenu()
        return
    }

    when(input.toInt()){
        1 -> SimpleProducer().start()
        2 -> SimpleConsumer().start()
        3 -> WordCounter().start()
    }
}

fun menuNumberIsValid(input:String?, validRange: IntRange): Boolean {
    val isDigitOnly = input?.all { c -> c.isDigit() } == true

    if(isDigitOnly)
         return validRange.contains(input!!.toInt())

    return false
}



