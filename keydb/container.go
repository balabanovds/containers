package keydb

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"

	"github.com/go-redis/redis/v8"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"

	"github.com/balabanovds/containers/internal/pool"
)

type KeydbContainer struct {
	host     string
	extPort  int
	teardown func()
	log      *log.Logger
}

type Opt func(*KeydbContainer)

// StartContainer ..
func StartContainer(ctx context.Context, opts ...Opt) (*KeydbContainer, error) {
	const (
		defaultHost    = "0.0.0.0"
		defaultExtPort = 6379
	)

	var err error

	c := KeydbContainer{
		host:    defaultHost,
		extPort: defaultExtPort,
		log:     log.New(os.Stdout, "[KeydbContainer] ", log.Flags()),
	}

	for _, opt := range opts {
		opt(&c)
	}

	if os.Getenv("GITLAB_CI") != "" {
		err = c.runCI(ctx)
	} else {
		err = c.runLocal(ctx)
	}

	return &c, err
}

func (c *KeydbContainer) Address() string {
	return net.JoinHostPort(c.host, strconv.Itoa(c.extPort))
}

func (c *KeydbContainer) Port() int {
	return c.extPort
}

func (c *KeydbContainer) Close() {
	if c == nil {
		return
	}

	c.teardown()
}

func (c *KeydbContainer) runLocal(_ context.Context) error {
	c.log.Println("running keydb container creation on local")
	var err error
	p := pool.Get()

	const containerName = "keydb_test"

	res, ok := p.Pool().ContainerByName(containerName)
	if ok {
		err = p.Pool().Purge(res)
		if err != nil {
			return err
		}
	}
	res, err = p.Pool().RunWithOptions(&dockertest.RunOptions{
		Name:         containerName,
		Repository:   "eqalpha/keydb",
		Tag:          "alpine_x86_64_v6.3.2",
		ExposedPorts: []string{"6379"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"6379/tcp": {
				{HostIP: c.host, HostPort: tcpPort(c)},
			},
		},
		NetworkID: p.Network().Network.ID,
		Platform:  "linux/amd64",
	})
	if err != nil {
		return fmt.Errorf("could not start resource: %w", err)
	}
	c.log.Printf("started container name=%s, id=%s, state=%s\n",
		res.Container.Name, res.Container.ID, res.Container.State.StateString())

	c.extPort, err = strconv.Atoi(res.GetPort("6379/tcp"))
	if err != nil {
		return err
	}

	retryFunc := func() error {
		cl := redis.NewClient(&redis.Options{
			Addr: c.Address(),
		})

		return cl.Ping(cl.Context()).Err()
	}

	err = p.Pool().Retry(retryFunc)
	if err != nil {
		return fmt.Errorf("could not connect to keydb: %w", err)
	}

	c.log.Println("keydb container is ready")

	c.teardown = func() {
		_ = p.Pool().Purge(res)
	}

	return nil
}

func (c *KeydbContainer) runCI(_ context.Context) error {
	panic("unmplemented")
}

// WithPort define custom external port for container bindings.
func WithPort(port int) Opt {
	return func(kc *KeydbContainer) {
		kc.extPort = port
	}
}

// WithLogger use custom logger
func WithLogger(l *log.Logger) Opt {
	return func(kc *KeydbContainer) {
		kc.log = l
	}
}

func tcpPort(p interface{ Port() int }) string {
	return fmt.Sprintf("%d/tcp", p.Port())
}
