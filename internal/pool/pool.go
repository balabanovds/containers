package pool

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/ory/dockertest/v3"
)

var (
	oncePool sync.Once
	p        *pool
)

type Pool interface {
	Pool() *dockertest.Pool
	Network() *dockertest.Network
	io.Closer
}

type pool struct {
	endpoint    string
	pool        *dockertest.Pool
	network     *dockertest.Network
	networkName string
	log         *log.Logger
}

type OptPool func(*pool)

func Get(opts ...OptPool) Pool {
	var err error
	oncePool.Do(func() {
		const (
			defaultEndpoint    = ""
			defaultNetworkName = "test"
		)

		p = &pool{
			endpoint:    defaultEndpoint,
			networkName: defaultNetworkName,
			log:         log.New(os.Stderr, "[DockerPool] ", log.Flags()),
		}

		for _, opt := range opts {
			opt(p)
		}

		p.pool, err = dockertest.NewPool(p.endpoint)
		if err != nil {
			panic(err)
		}

		if ferr := p.pool.Client.Ping(); ferr != nil {
			panic(ferr)
		}

		networks, ferr := p.pool.Client.ListNetworks()
		if ferr != nil {
			p.log.Printf("failed to get list networks: %s", ferr)
		} else {
			for _, n := range networks {
				if n.Name == p.networkName {
					ferr = p.pool.Client.RemoveNetwork(n.ID)
					if ferr != nil {
						p.log.Printf("error to delete network id=%s, err: %s\n", n.ID, ferr)
					} else {
						p.log.Printf("deleted network id=%s\n", n.ID)
					}
				}
			}
		}

		p.network, ferr = p.pool.CreateNetwork(p.networkName)
		if ferr != nil {
			panic(ferr)
		}
	})

	if err != nil {
		panic(err)
	}

	return p
}

func (p *pool) Pool() *dockertest.Pool {
	return p.pool
}

func (p *pool) Network() *dockertest.Network {
	return p.network
}

func (p *pool) Close() error {
	return p.pool.Client.RemoveNetwork(p.network.Network.ID)
}

// WithEndpoint use custom endpoint.
func WithEndpoint(endpoint string) OptPool {
	return func(p *pool) {
		p.endpoint = endpoint
	}
}

// WithNetworkName use custom network name.
func WithNetworkName(name string) OptPool {
	return func(p *pool) {
		p.networkName = name
	}
}
