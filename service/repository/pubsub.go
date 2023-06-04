package repository

import (
	"context"

	"excalibur-platform/stream-cdc-mysql/domain"

	"cloud.google.com/go/pubsub"
	"github.com/siddontang/go-log/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type pubSubRepository struct {
	Ctx                context.Context
	pubsubClient       *pubsub.Client
	pubsubSchemaClient *pubsub.SchemaClient
}

func NewPubSubRepository(
	pubsubClient *pubsub.Client,
	pubsubSchemaClient *pubsub.SchemaClient,
) domain.PubSubRepository {

	return &pubSubRepository{
		Ctx:                context.Background(),
		pubsubClient:       pubsubClient,
		pubsubSchemaClient: pubsubSchemaClient,
	}

}

func (repo *pubSubRepository) GetSchema(id string) (*string, error) {

	var err error
	var schemaConfig *pubsub.SchemaConfig

	schemaConfig, err = repo.pubsubSchemaClient.Schema(
		repo.Ctx,
		id,
		pubsub.SchemaViewFull,
	)

	if err != nil {
		log.Errorf("[Pubsub Repository][Get Schema] Err : %s\n", err.Error())
		return nil, err
	}

	return &schemaConfig.Definition, nil

}

func (repo *pubSubRepository) PublishMessage(topicName string, message protoreflect.ProtoMessage) error {

	var err error
	var topic *pubsub.Topic = repo.pubsubClient.Topic(topicName)

	var msg []byte

	msg, err = protojson.Marshal(message)

	if err != nil {
		log.Errorf("[Pubsub Repository][Publish Message] Err : %s\n", err.Error())
		return err
	}

	var result *pubsub.PublishResult = topic.Publish(
		repo.Ctx,
		&pubsub.Message{
			Data: []byte(msg),
		},
	)

	_, err = result.Get(repo.Ctx)

	if err != nil {
		log.Errorf("[Pubsub Repository][Publish Message] Err : %s\n", err.Error())
		return err
	}

	return nil

}
