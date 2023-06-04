package utils

import (
	"context"

	"cloud.google.com/go/storage"
)

func NewGCSClient(ctx context.Context) (*storage.Client, error) {

	var err error
	var client *storage.Client

	client, err = storage.NewClient(ctx)

	if err != nil {
		return nil, err
	}

	return client, nil

}
