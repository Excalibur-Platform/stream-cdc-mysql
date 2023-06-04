package domain

import "google.golang.org/protobuf/reflect/protoreflect"

type PubSubRepository interface {
	GetSchema(id string) (*string, error)
	PublishMessage(topicName string, message protoreflect.ProtoMessage) error
}
