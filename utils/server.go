package utils

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/go-mysql-org/go-mysql/canal"
)

type Server struct {
	Config             *Config
	Ctx                context.Context
	Canal              *canal.Canal
	PubsubClient       *pubsub.Client
	PubsubSchemaClient *pubsub.SchemaClient
	GcsClient          *storage.Client
}

func NewServer() (*Server, error) {

	var err error
	var Config *Config

	Config, err = NewConfig()

	if err != nil {
		return nil, err
	}

	var server *Server = &Server{
		Config: Config,
		Ctx:    context.Background(),
	}

	err = server.NewCanal()

	if err != nil {
		return nil, err
	}

	var pubsubClient *pubsub.Client
	var pubsubSchemaClient *pubsub.SchemaClient

	pubsubClient, pubsubSchemaClient, err = NewPubSubClient(
		server.Ctx,
		server.Config.PubSubProjectID,
	)

	if err != nil {
		return nil, err
	}

	server.PubsubClient = pubsubClient
	server.PubsubSchemaClient = pubsubSchemaClient

	var gcsClient *storage.Client

	gcsClient, err = NewGCSClient(server.Ctx)

	if err != nil {
		return nil, err
	}

	server.GcsClient = gcsClient

	return server, nil

}

func (s *Server) Close() {

	s.Canal.Close()
	s.PubsubClient.Close()
	s.PubsubSchemaClient.Close()
	s.GcsClient.Close()

}

func (s *Server) NewCanal() error {

	var err error

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", s.Config.SourceHost, s.Config.SourcePort)
	cfg.User = s.Config.SourceUser
	cfg.Password = s.Config.SourcePassword
	cfg.Dump.TableDB = s.Config.SourceDatabaseName
	cfg.Dump.Tables = s.Config.SourceTables
	cfg.Flavor = s.Config.SourceType
	cfg.Dump.MaxAllowedPacketMB = s.Config.MaxAllowedPacketMB

	canal, err := canal.NewCanal(cfg)

	if err != nil {
		return err
	}

	s.Canal = canal

	return nil

}
