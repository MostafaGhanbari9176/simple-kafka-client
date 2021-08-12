fun main(args: Array<String>) {

    mainMenu()

}

private fun mainMenu() {
    println("please choose one : ")
    println("1- simple producer")

    val input = readLine()

    if (input.isNullOrEmpty()) {
        mainMenu()
        return
    }

    SimpleProducer().start()
}



