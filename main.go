package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// Kafka 配置常量
const (
	BootstrapServers       = "192.168.140.128:9092" // Kafka 服务器地址
	TopicName              = "test-topic"           // 主题名称
	GroupID                = "test-group"           // 消费者组ID
	Partition        int32 = 0                      // 分区ID
)

type TopicInfo struct {
	TopicName string
	sarama.TopicDetail
}

var _ KafkaManager = (*kafkaManager)(nil)

type KafkaManager interface {
	// 获取消息管道
	GetMessages() chan *sarama.ConsumerMessage
	// 获取ClusterAdmin
	GetClusterAdmin() sarama.ClusterAdmin
	// 获取Producer
	GetProducer() sarama.SyncProducer
	// 获取Consumer
	GetConsumer() sarama.Consumer
	// 获取ConsumerGroup
	GetConsumerGroup() sarama.ConsumerGroup
	// 创建 Kafka 生产者配置
	CreateProducerConfig()
	// 创建 Kafka 消费者配置
	CreateConsumerConfig()
	// 创建 Kafka 生产者
	CreateProducer() error
	// 创建 Kafka 消费者（分区模式）
	CreatePartitionConsumer() error
	// 创建 Kafka 消费者组消费者
	CreateConsumerGroup() error
	// 发送消息到 Kafka
	ProduceMessage(key, value string) error
	// 批量发送消息
	ProduceMessages(messages map[string]string) error
	// 消费消息（分区模式）
	ConsumeMessagesPartition(topicName string, partition int32, ctx context.Context) error
	// 消费消息（消费者组模式）
	ConsumeMessagesGroup(topics []string, ctx context.Context) error
	// 创建主题（如果不存在）
	CreateTopicIfNotExists(topicName string) error
	// 获取主题信息
	GetTopicInfo(topicName string) (*TopicInfo, error)
	// 创建集群管理员
	CreateClusterAdmin() error
	// 关闭kafka连接
	Close()
}

// 定于Kakfa示例结构体
type kafkaManager struct {
	ClusterAdmin   sarama.ClusterAdmin
	AdminConfig    *sarama.Config
	Producer       sarama.SyncProducer
	ProducerConfig *sarama.Config
	Consumer       sarama.Consumer
	GroupID        string
	ConsumerGroup  sarama.ConsumerGroup
	ConsumerConfig *sarama.Config
	Messages       chan *sarama.ConsumerMessage
	Version        sarama.KafkaVersion
}

// 泛型版本，适用于任何可比较的类型
func contains[T comparable](slice []T, target T) bool {
	for _, item := range slice {
		if item == target {
			return true
		}
	}
	return false
}

func NewKafkaManager(GroupID string, Version sarama.KafkaVersion) KafkaManager {
	if GroupID == "" || !contains(sarama.SupportedVersions, Version) {
		return nil
	}

	return &kafkaManager{
		GroupID:     GroupID,
		Version:     Version,
		AdminConfig: sarama.NewConfig(),
		Messages:    make(chan *sarama.ConsumerMessage, 10),
	}
}

// 获取消息管道
func (km *kafkaManager) GetMessages() chan *sarama.ConsumerMessage {
	return km.Messages
}

// 获取ClusterAdmin
func (km *kafkaManager) GetClusterAdmin() sarama.ClusterAdmin {
	return km.ClusterAdmin
}

// 获取Producer
func (km *kafkaManager) GetProducer() sarama.SyncProducer {
	return km.Producer
}

// 获取Consumer
func (km *kafkaManager) GetConsumer() sarama.Consumer {
	return km.Consumer
}

// 获取ConsumerGroup
func (km *kafkaManager) GetConsumerGroup() sarama.ConsumerGroup {
	return km.ConsumerGroup
}

// 创建 Kafka 生产者配置
func (km *kafkaManager) CreateProducerConfig() {
	config := sarama.NewConfig()

	// 生产者配置
	config.Producer.RequiredAcks = sarama.WaitForAll         // 等待所有副本确认
	config.Producer.Retry.Max = 3                            // 重试次数
	config.Producer.Return.Successes = true                  // 返回成功消息
	config.Producer.Compression = sarama.CompressionSnappy   // 压缩方式
	config.Producer.Flush.Frequency = 500 * time.Millisecond // 刷新频率

	// 通用配置
	config.Version = sarama.V4_0_0_0 // Kafka 版本

	km.ProducerConfig = config
}

// 创建 Kafka 消费者配置
func (km *kafkaManager) CreateConsumerConfig() {
	config := sarama.NewConfig()

	// 消费者配置
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true // 自动提交偏移量
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest // 从最新消息开始消费

	// 通用配置
	config.Version = sarama.V4_0_0_0

	km.ConsumerConfig = config
}

// 创建 Kafka 生产者
func (km *kafkaManager) CreateProducer() error {
	km.CreateProducerConfig()

	// 创建同步生产者
	var err error
	km.Producer, err = sarama.NewSyncProducer([]string{BootstrapServers}, km.ProducerConfig)
	if err != nil {
		return fmt.Errorf("创建生产者失败: %v", err)
	}

	log.Println("Kafka 生产者创建成功")
	return nil
}

// 创建 Kafka 消费者（分区模式）
func (km *kafkaManager) CreatePartitionConsumer() error {
	km.CreateConsumerConfig()

	var err error
	km.Consumer, err = sarama.NewConsumer([]string{BootstrapServers}, km.ConsumerConfig)
	if err != nil {
		return fmt.Errorf("创建消费者失败: %v", err)
	}

	log.Println("Kafka 分区消费者创建成功")
	return nil
}

// 创建 Kafka 消费者组消费者
func (km *kafkaManager) CreateConsumerGroup() error {
	km.CreateConsumerConfig()

	var err error
	km.ConsumerGroup, err = sarama.NewConsumerGroup([]string{BootstrapServers}, km.GroupID, km.ConsumerConfig)
	if err != nil {
		return fmt.Errorf("创建消费者组失败: %v", err)
	}

	log.Printf("Kafka 消费者组创建成功: %s", GroupID)
	return nil
}

// 发送消息到 Kafka
func (km *kafkaManager) ProduceMessage(key, value string) error {
	message := &sarama.ProducerMessage{
		Topic: TopicName,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	// 发送消息
	partition, offset, err := km.Producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("发送消息失败: %v", err)
	}

	log.Printf("消息发送成功: 分区=%d, 偏移量=%d, 键=%s, 值=%s",
		partition, offset, key, value)
	return nil
}

// 批量发送消息
func (km *kafkaManager) ProduceMessages(messages map[string]string) error {
	for key, value := range messages {
		err := km.ProduceMessage(key, value)
		if err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond) // 短暂延迟
	}
	return nil
}

// 分区消费者处理器
type PartitionConsumerHandler struct {
	messagesChan chan *sarama.ConsumerMessage
}

// 当消费者加入消费者组时调用,可以用于初始化资源
func (h PartitionConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// 当消费者退出消费者组时调用,可以用于清理资源
func (h PartitionConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// 负责实际的消息消费逻辑
func (h PartitionConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		log.Printf("收到消息: 主题=%s, 分区=%d, 偏移量=%d, 键=%s, 值=%s",
			message.Topic, message.Partition, message.Offset,
			string(message.Key), string(message.Value))
		// 将message输出到管道
		h.messagesChan <- message

		// 标记消息为已处理,如果不调用此方法，偏移量不会自动提交
		session.MarkMessage(message, "")
	}
	return nil
}

// 消费消息（分区模式）
func (km *kafkaManager) ConsumeMessagesPartition(topicName string, partition int32, ctx context.Context) error {
	log.Printf("开始消费主题 %s 的消息（分区模式）...", topicName)

	// 获取主题的分区列表
	partitions, err := km.Consumer.Partitions(topicName)
	if err != nil {
		return fmt.Errorf("获取分区列表失败: %v", err)
	}

	log.Printf("主题 %s 的分区数量: %d", topicName, len(partitions))
	go func() {
		// 创建分区消费者，从最新偏移量开始
		partitionConsumer, err := km.Consumer.ConsumePartition(topicName, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("创建分区 %d 消费者失败: %v", partition, err)
			return
		}

		log.Printf("开始消费分区 %d 的消息", partition)

		for {
			select {
			case msg := <-partitionConsumer.Messages():
				log.Printf("分区 %d - 收到消息: 偏移量=%d, 键=%s, 值=%s",
					partition, msg.Offset, string(msg.Key), string(msg.Value))
				km.Messages <- msg

			case err := <-partitionConsumer.Errors():
				log.Printf("分区 %d 消费错误: %v", partition, err)
				return
			case <-ctx.Done():
				log.Print("消费者退出")
				partitionConsumer.Close()
				return
			}
		}
	}()

	return nil
}

// 消费消息（消费者组模式）
func (km *kafkaManager) ConsumeMessagesGroup(topics []string, ctx context.Context) error {
	log.Printf("开始消费主题 %s 的消息（消费者组模式）...\n", topics)

	handler := &PartitionConsumerHandler{
		messagesChan: km.Messages,
	}

	// 消费者组消费
	err := km.ConsumerGroup.Consume(ctx, topics, handler)
	if err != nil {
		log.Printf("消费者组模式消费失败: %v", err)
		return fmt.Errorf("消费者组消费失败: %v", err)
	}

	return nil
}

// 创建主题（如果不存在）
func (km *kafkaManager) CreateTopicIfNotExists(topicName string) error {
	// 检查主题是否已存在
	topicInfo, _ := km.GetTopicInfo(topicName)
	if topicInfo != nil {
		return nil
	}

	// 创建主题配置
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	// 创建主题
	err := km.ClusterAdmin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		return fmt.Errorf("创建主题失败: %v", err)
	}

	log.Printf("主题 %s 创建成功", TopicName)
	return nil
}

// 获取主题信息
func (km *kafkaManager) GetTopicInfo(topicName string) (*TopicInfo, error) {
	topics, err := km.ClusterAdmin.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("获取主题信息失败: %v", err)
	}

	if topic, exists := topics[topicName]; exists {
		topicInfo := &TopicInfo{
			TopicName: topicName,
			TopicDetail: sarama.TopicDetail{
				NumPartitions:     topic.NumPartitions,
				ReplicationFactor: topic.ReplicationFactor,
			},
		}

		log.Printf("主题 %s 存在, 分区数: %d, 副本数: %d",
			topicInfo.TopicName, topicInfo.NumPartitions, topicInfo.ReplicationFactor)
		return topicInfo, nil
	}
	log.Printf("主题 %s 不存在", topicName)
	return nil, fmt.Errorf("主题 %s 不存在", topicName)
}

// 创建集群管理员
func (km *kafkaManager) CreateClusterAdmin() error {
	km.AdminConfig.Version = km.Version
	var err error
	km.ClusterAdmin, err = sarama.NewClusterAdmin([]string{BootstrapServers}, km.AdminConfig)
	if err != nil {
		return fmt.Errorf("创建集群管理员失败: %v", err)
	}

	log.Println("Kafka 集群管理员创建成功")
	return nil
}

// 关闭kafka连接
func (km *kafkaManager) Close() {
	if km.Producer != nil {
		km.Producer.Close()
	}
	if km.Consumer != nil {
		km.Consumer.Close()
	}
	if km.ConsumerGroup != nil {
		km.ConsumerGroup.Close()
	}
	if km.ClusterAdmin != nil {
		km.ClusterAdmin.Close()
	}
	log.Println("Kafka 连接关闭成功")
}

// 主函数
func main() {
	log.Println("开始 Kafka 4.0 示例程序（使用 Sarama）")

	// 设置信号处理，优雅关闭
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	// 1. 初始化Kafka管理器
	km := NewKafkaManager(GroupID, sarama.V4_0_0_0)
	if km == nil {
		log.Fatalf("创建 Kafka 管理器失败: 无效的 GroupID(%v) 或 Version(%v)", GroupID, sarama.V4_0_0_0)
	}

	// 2. 创建集群管理员
	err := km.CreateClusterAdmin()
	if err != nil {
		log.Fatalf("创建集群管理员失败: %v\n", err)
	}

	// 3. 创建主题（如果不存在）
	err = km.CreateTopicIfNotExists(TopicName)
	if err != nil {
		log.Fatalf("创建主题失败（可能已存在）: %v\n", err)
	}

	// 4. 获取主题信息
	_, err = km.GetTopicInfo(TopicName)
	if err != nil {
		log.Fatalf("获取主题信息失败: %v\n", err)
	}

	// 5. 创建生产者
	err = km.CreateProducer()
	if err != nil {
		log.Fatalf("创建生产者失败: %v\n", err)
	}

	// 6. 消费模式选择
	log.Println("\n=== 选择消费模式 ===")
	log.Println("1. 分区模式消费（不使用消费者组）")
	log.Println("2. 消费者组模式消费")

	go func(ctx context.Context) {
		msg := km.GetMessages()
		for {
			select {
			case <-ctx.Done():
				log.Print("管道退出")
				close(msg)
				return
			case msg := <-msg:
				log.Printf("从通道收到消息: 偏移量=%d, 键=%s, 值=%s",
					msg.Offset, string(msg.Key), string(msg.Value))
			}
		}
	}(ctx)

	// 示例：分区模式消费
	log.Println("\n=== 示例：分区模式消费 ===")
	err = km.CreatePartitionConsumer()
	if err != nil {
		log.Fatalf("创建分区消费者失败: %v\n", err)
	}

	err = km.ConsumeMessagesPartition(TopicName, Partition, ctx)
	if err != nil {
		log.Printf("分区模式消费失败: %v\n", err)
	}

	// // 示例: 消费者组模式消费
	// log.Println("\n=== 示例：消费者组模式消费 ===")
	// err = km.CreateConsumerGroup()
	// if err != nil {
	// 	log.Fatalf("创建消费者组失败: %v\n", err)
	// }

	// go km.ConsumeMessagesGroup([]string{TopicName}, ctx)

	// defer km.Close()

	// 6. 发送测试消息
	timestamp1 := strconv.FormatInt(time.Now().UnixNano(), 10)
	timestamp2 := strconv.FormatInt(time.Now().UnixNano()+1, 10)
	timestamp3 := strconv.FormatInt(time.Now().UnixNano()+2, 10)

	testMessages := map[string]string{
		"key_" + timestamp1: "Sarama_测试消息_1-" + timestamp1,
		"key_" + timestamp2: "Sarama_测试消息_2-" + timestamp2,
		"key_" + timestamp3: "Sarama_测试消息_3-" + timestamp3,
	}

	time.Sleep(3 * time.Second)
	err = km.ProduceMessages(testMessages)
	if err != nil {
		log.Printf("发送消息失败: %v\n", err)
	}

	// 等待信号，优雅退出
	<-signals
	cancel()
	log.Println("程序收到退出信号，正在关闭...")
	log.Println("程序执行完成")
}
