package main

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatal("%s: %s", msg, err)
	}
}

type Cripto struct {
	Nome       string
	Valor      float32
	Quantidade float32
}
type CriptoExchange struct {
	Nome       string
	Valor      float32
	Quantidade float32
	Message    string
}

func main() {
	conn, err := amqp.Dial("amqp://rabbitmq:rabbitmq@localhost:5672/")
	failOnError(err, "Ocorreu um erro ao se conectar com o rabbitmq")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Ocorreu um erro ao abrir o canal do rabbit")

	queue_message, err := ch.QueueDeclare("send-queue-test", false, false, false, false, nil)
	failOnError(err, "Ocorreu um erro ao criar a fila")

	err = ch.ExchangeDeclare("fanout_exchange", "fanout", true, false, false, false, nil)
	failOnError(err, "Ocorreu um erro ao criar a exhange")

	cripto := Cripto{Nome: "Bitcoin", Valor: 55000.00, Quantidade: 0.10000000}
	body, err := json.Marshal(cripto)
	failOnError(err, "Ocorreu um erro ao serializar o seu objeto")

	err = ch.Publish("", queue_message.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: body})
	failOnError(err, "erro ao publicar a mensagem.")

	msgs, err := ch.Consume(queue_message.Name, "", true, false, false, false, nil)
	failOnError(err, "erro ao consumir msg.")
	forever := make(chan bool)
	go func() {
		for message := range msgs {
			log.Printf("Mensagem recebida: %s", message.Body)
			criptoExchange := CriptoExchange{}
			err = json.Unmarshal(message.Body, &criptoExchange)
			criptoExchange.Message = "mensagem recebida e republicada."
			failOnError(err, "Ocorreu um erro ao deserializar o seu objeto")

			exchangebody, err := json.Marshal(criptoExchange)
			failOnError(err, "Ocorreu um erro ao serializar o seu objeto")

			ch.Publish("fanout_exchange", "*", false, false, amqp.Publishing{ContentType: "text/plain", Body: exchangebody})

		}
	}()
	log.Printf("wating for messages.")
	<-forever
}
