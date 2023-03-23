package kafka

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/balabanovds/containers/internal/pool"
)

type zookeeperContainer struct {
	port     int
	teardown func()
}

type KafkaContainer struct {
	zookeeper zookeeperContainer
	topic     string
	extPort   int
	extIP     string
	teardown  func()
	log       *log.Logger
}

// StartKafkaContainer ..
func StartContainer(ctx context.Context, opts ...Opt) (*KafkaContainer, error) {
	const (
		defaultZKPort = 2181
		defaultPort   = 19092
		defaultTopic  = "test_topic"
	)

	c := KafkaContainer{
		zookeeper: zookeeperContainer{
			port: defaultZKPort,
		},
		topic:   defaultTopic,
		extPort: defaultPort,
		log:     log.New(os.Stdout, "[KafkaContainer] ", log.Flags()),
	}

	for _, opt := range opts {
		opt(&c)
	}

	var err error
	if os.Getenv("GITLAB_CI") != "" {
		err = c.runCI(ctx)
	} else {
		err = c.runLocal(ctx)
	}
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *KafkaContainer) Host() string {
	return c.extIP
}

func (c *KafkaContainer) Port() int {
	return c.extPort
}

func (c *KafkaContainer) Address() string {
	return fmt.Sprintf("%s:%d", c.Host(), c.Port())
}

func (c *KafkaContainer) Topic() string {
	return c.topic
}

func (c *KafkaContainer) Close() {
	if c == nil {
		return
	}
	c.teardown()
	c.zookeeper.teardown()

	_ = pool.Get().Close()
}

func (c *KafkaContainer) runLocal(ctx context.Context) error {
	if err := c.runZookeeper(ctx); err != nil {
		return fmt.Errorf("error starting zookeper: %s", err)
	}

	if err := c.runKafka(ctx); err != nil {
		return fmt.Errorf("error starting kafka: %s", err)
	}

	return nil
}

func (c *KafkaContainer) runCI(_ context.Context) error {
	panic("unimplemented")
}

func (c *KafkaContainer) runZookeeper(_ context.Context) error {
	c.log.Println("running zookeeper container creation on local")
	p := pool.Get()

	const containerName = "zookeeper_test"

	var err error
	res, ok := p.Pool().ContainerByName(containerName)
	if ok {
		err = p.Pool().Purge(res)
		if err != nil {
			return err
		}
	}

	res, err = p.Pool().RunWithOptions(&dockertest.RunOptions{
		Name:       containerName,
		Repository: "wurstmeister/zookeeper",
		// Tag:          "3.4.6",
		ExposedPorts: []string{"2181/tcp"},
		NetworkID:    p.Network().Network.ID,
		Hostname:     "zookeeper",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"2181/tcp": {{HostIP: "localhost", HostPort: "2181/tcp"}},
		},
	})
	if err != nil {
		return fmt.Errorf("error creating docker resource: %w", err)
	}
	c.log.Printf("zookeeper container created name=%s id=%s state=%s\n",
		res.Container.Name, res.Container.ID, res.Container.State.StateString())

	c.zookeeper.port, err = strconv.Atoi(res.GetPort("2181/tcp"))
	if err != nil {
		return fmt.Errorf("error parsing port: %s", err)
	}

	nopLogger := log.New(io.Discard, "", log.Flags())

	// Make sure ZK is up and running
	con, _, err := zk.Connect(
		[]string{fmt.Sprintf("127.0.0.1:%d", c.zookeeper.port)},
		10*time.Second,
		zk.WithLogger(nopLogger),
	)
	if err != nil {
		return fmt.Errorf("error connecting to zookeeper: %w", err)
	}
	defer con.Close()

	retryFn := func() error {
		switch con.State() {
		case zk.StateConnected, zk.StateHasSession:
			return nil
		default:
			return fmt.Errorf("not ready for connection, state=%s", con.State().String())
		}
	}

	err = p.Pool().Retry(retryFn)
	if err != nil {
		return err
	}

	c.log.Println("zookeeper is ready to accept connections")

	c.zookeeper.teardown = func() {
		_ = p.Pool().Purge(res)
	}

	return nil
}

func (c *KafkaContainer) runKafka(ctx context.Context) error {
	c.log.Println("running kafka container on local")
	p := pool.Get()

	env := []string{
		fmt.Sprintf("KAFKA_ZOOKEEPER_CONNECT=zookeeper:%d", c.zookeeper.port),
		fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=INSIDE://kafka:9092,OUTSIDE://localhost:%d", c.extPort),
		fmt.Sprintf("KAFKA_LISTENERS=INSIDE://:9092,OUTSIDE://:%d", c.extPort),
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT",
		"KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"KAFKA_AUTO_CREATE_TOPICS_ENABLE=TRUE",
		fmt.Sprintf("KAFKA_CREATE_TOPICS=%s:1:1", c.topic),
	}
	const (
		containerName = "kafka_test"
	)

	port := tcpPort(c)

	var err error
	res, ok := p.Pool().ContainerByName(containerName)
	if ok {
		err = p.Pool().Purge(res)
		if err != nil {
			return err
		}
	}
	res, err = p.Pool().RunWithOptions(&dockertest.RunOptions{
		Name:         containerName,
		Repository:   "wurstmeister/kafka",
		Env:          env,
		ExposedPorts: []string{port},
		NetworkID:    p.Network().Network.ID,
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(port): {{HostIP: "localhost", HostPort: port}},
		},
		Hostname: "kafka",
	})
	if err != nil {
		return err
	}
	c.log.Printf("kafka container created name=%s id=%s state=%s\n",
		res.Container.Name, res.Container.ID, res.Container.State.StateString())

	// Check it is up and running
	retryFunc := func() error {
		con, ferr := kafkago.DialLeader(ctx, "tcp", c.Address(), c.topic, 0)
		if ferr != nil {
			return ferr
		}
		_, ferr = con.ApiVersions()
		if ferr != nil {
			return ferr
		}

		c.log.Println("Kafka cluster is ready to go")
		return nil
	}

	err = p.Pool().Retry(retryFunc)
	if err != nil {
		return err
	}

	c.teardown = func() {
		_ = p.Pool().Purge(res)
	}

	return nil
}

func tcpPort(p interface{ Port() int }) string {
	return fmt.Sprintf("%d/tcp", p.Port())
}
