package utils

import (
	"context"

	"cloud.google.com/go/pubsub"
)

func NewPubSubClient(ctx context.Context, projectID string) (*pubsub.Client, *pubsub.SchemaClient, error) {

	var err error
	var pubSubClient *pubsub.Client
	var pubSubSchemaClient *pubsub.SchemaClient

	pubSubClient, err = pubsub.NewClient(ctx, projectID)

	if err != nil {
		return nil, nil, err
	}

	pubSubSchemaClient, err = pubsub.NewSchemaClient(ctx, projectID)

	if err != nil {
		return nil, nil, err
	}

	return pubSubClient, pubSubSchemaClient, nil

}
