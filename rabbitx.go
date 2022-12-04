package rabbitx

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// Rabbitx 用于封装对 RabbitMQ 的操作
type Rabbitx struct {
	conn *amqp.Connection
}

// New 创建一个 Rabbitx 实例
func New(username string, password string, host string, port string) (*Rabbitx, error) {
	// 构建连接地址：amqp://username:password@host:port/
	uriFmt := "amqp://%s:%s@%s:%s/"
	uri := fmt.Sprintf(uriFmt, username, password, host, port)

	// 建立连接
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, err
	}

	return &Rabbitx{conn: conn}, nil
}

// Channel 获取 channel
func (r *Rabbitx) Channel() (*amqp.Channel, error) {
	channel, err := r.conn.Channel()
	if err != nil {
		return nil, err
	}
	return channel, nil
}

// MsgHandler 表示消息处理的接口
type MsgHandler interface {
	handle(amqp.Delivery)
}

// Consume 消费队列里的消息
func (r *Rabbitx) Consume(queueName string, autoAck bool, handler MsgHandler) error {
	channel, err := r.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	deliveries, err := channel.Consume(
		queueName,
		"consumer-of-"+queueName,
		autoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// 打印消息体
	for delivery := range deliveries {
		// 消息处理，handleFunc 无需负责 ack
		handler.handle(delivery)
		if autoAck == false {
			errAck := delivery.Ack(false)
			if errAck != nil {
				// TODO: 这里的处理方式存疑
				log.Printf("ack error: %s\n", err)
			}
		}
	}

	return nil
}

// DeclareExchange 声明 exchange
func (r *Rabbitx) DeclareExchange(exchangeName string, exchangeType string) error {
	// 获取 channel
	channel, err := r.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	// 声明 exchange
	err = channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	return nil
}

// DeclareQueue 声明 queue
func (r *Rabbitx) DeclareQueue(queueName string) (*amqp.Queue, error) {
	// 获取 channel
	channel, err := r.Channel()
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	// 声明 queue
	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &queue, nil
}

// Bind 绑定 queue 到 exchange
func (r *Rabbitx) Bind(queueName string, exchangeName string, routingKey string) error {
	// 获取 channel
	channel, err := r.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	// 绑定 exchange 和 queue
	err = channel.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	return nil
}
