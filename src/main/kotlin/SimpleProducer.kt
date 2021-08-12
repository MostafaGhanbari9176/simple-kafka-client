import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class SimpleProducer {

    private var topic: String? = ""
    private lateinit var producer: KafkaProducer<String, String>

    fun start() {
        println("-".repeat(25))
        println("simple produser")
        println("-".repeat(5))

        println("inter topic name")

        topic = readLine()
        if (topic.isNullOrEmpty()) {
            start()
            return
        }

        val props = Properties().apply {
            put("bootstrap.servers", "127.0.0.1:9093")
            put("acks", "all")
            put("retries", "0")
            put("batch.size", 16384)
            put("linger.ms", 10)
            put("buffer.memory", 33554432)
            put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        }

        producer = KafkaProducer<String, String>(props)

        getData()

    }

    private fun getData() {
        println("inter record as key:value")
        val input = readLine()
        if (input.isNullOrEmpty()) {
            getData()
            return
        }

        val key = input.substring(0, input.indexOf(':'))
        val value = input.substring(input.indexOf(':') + 1)

        producer.send(
            ProducerRecord<String, String>(topic, key, value)
        ) { metadata, exception ->
            if (exception != null)
                println("a exception occurred:${exception.message} for message:$input")
        }

        getData()
    }


}


