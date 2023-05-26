package postgres

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/balabanovds/containers/internal/pool"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

type PostgresContainer struct {
	host       string
	extPort    int
	imageTag   string
	dbname     string
	dbuser     string
	dbpassword string
	db         *pgxpool.Pool
	log        *log.Logger
	teardown   func()
}

type Opt func(*PostgresContainer)

// StartContainer ..
func StartContainer(ctx context.Context, opts ...Opt) (*PostgresContainer, error) {
	const (
		defaultHost       = "0.0.0.0"
		defaultExtPort    = 5432
		defaultDBName     = "test_db"
		defaultDBUser     = "postgres"
		defaultDBPassword = "123456"
		defaultTag        = "14-alpine"
	)

	var err error

	c := PostgresContainer{
		host:       defaultHost,
		extPort:    defaultExtPort,
		imageTag:   defaultTag,
		dbname:     defaultDBName,
		dbuser:     defaultDBUser,
		dbpassword: defaultDBPassword, log: log.New(os.Stderr, "[PostgresContainer] ", log.Flags()),
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

func (c *PostgresContainer) DB(ctx context.Context) (*pgxpool.Pool, error) {
	if c.db == nil {
		dbCfg, err := pgxpool.ParseConfig(c.DSN())
		if err != nil {
			return nil, err
		}

		c.db, err = pgxpool.ConnectConfig(ctx, dbCfg)
		if err != nil {
			return nil, err
		}

		err = c.db.Ping(ctx)
		if err != nil {
			return nil, err
		}
		c.log.Println("database is reachable")
	}

	return c.db, nil
}

func (c *PostgresContainer) DSN() string {
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		c.host, c.extPort, c.dbname, c.dbuser, c.dbpassword,
	)
}

func (c *PostgresContainer) Port() int {
	return c.extPort
}

func (c *PostgresContainer) Close() {
	if c == nil {
		return
	}

	c.teardown()
}

func (c *PostgresContainer) runLocal(ctx context.Context) error {
	c.log.Println("running postgres container creation on local")
	var err error
	p := pool.Get()

	env := []string{
		"POSTGRES_USER=" + c.dbuser,
		"POSTGRES_PASSWORD=" + c.dbpassword,
		"POSTGRES_DB=" + c.dbname,
	}

	const containerName = "pg_test"

	res, ok := p.Pool().ContainerByName(containerName)
	if ok {
		err = p.Pool().Purge(res)
		if err != nil {
			return err
		}
	}
	res, err = p.Pool().RunWithOptions(&dockertest.RunOptions{
		Name:         containerName,
		Repository:   "postgres",
		Tag:          c.imageTag,
		Env:          env,
		ExposedPorts: []string{"5432"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {
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

	retryFunc := func() error {
		c.log.Println("geting db connection")
		db, ferr := c.DB(ctx)
		if ferr != nil {
			return ferr
		}

		c.log.Println("trying to connect to db")

		err = db.Ping(ctx)
		if err != nil {
			c.log.Println("could not connect to db")
			return nil
		}
		c.log.Println("connection successful")
		return nil
	}

	err = p.Pool().Retry(retryFunc)
	if err != nil {
		return fmt.Errorf("could not connect to postgres database: %w", err)
	}

	c.teardown = func() {
		c.db.Close()
		_ = p.Pool().Purge(res)
	}

	return nil
}

func (c *PostgresContainer) runCI(_ context.Context) error {
	panic("unmplemented")
}

// WithPostgresPort set custom port for container binding.
func WithPort(port int) Opt {
	return func(pc *PostgresContainer) {
		pc.extPort = port
	}
}

// WithPostgresDBName set custom db name.
func WithDBName(dbname string) Opt {
	return func(pc *PostgresContainer) {
		pc.dbname = dbname
	}
}

// WithPostgresCreds set custom creds.
func WithCreds(login, password string) Opt {
	return func(pc *PostgresContainer) {
		pc.dbuser = login
		pc.dbpassword = password
	}
}

// WithLogger use custom logger
func WithLogger(l *log.Logger) Opt {
	return func(kc *PostgresContainer) {
		kc.log = l
	}
}

// WithImageTag use custom docker image tag for postgres.
func WithImageTag(tag string) Opt {
	return func(pc *PostgresContainer) {
		pc.imageTag = tag
	}
}

func tcpPort(p interface{ Port() int }) string {
	return fmt.Sprintf("%d/tcp", p.Port())
}
