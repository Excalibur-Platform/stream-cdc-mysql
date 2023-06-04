package usecase

import (
	"sync"
	"time"

	"excalibur-platform/stream-cdc-mysql/domain"
	"excalibur-platform/stream-cdc-mysql/utils"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/siddontang/go-log/log"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type usecase struct {
	canal.DummyEventHandler
	serverConfig utils.Config

	pubSubRepository        domain.PubSubRepository
	pubSubMessgaeDescriptor map[string]protoreflect.MessageDescriptor
	pubSubTopic             map[string]string

	offsetBucketName   string
	offsetObjectPrefix string

	gtidMx                 sync.RWMutex
	lastGTIDOffsetStoredTs *time.Time

	binLogMX                    sync.RWMutex
	lastBinLogPosOffsetStoredTs *time.Time

	errMx sync.RWMutex
	err   error

	gcsRepository domain.GCSRepository
}

func NewUsecase(
	serverConfig utils.Config,
	pubSubRepository domain.PubSubRepository,
	gcsRepository domain.GCSRepository,
) (domain.Usecase, error) {

	log.Infof("[Usecase] Initiate Usecase Handler")

	u := &usecase{
		serverConfig:            serverConfig,
		pubSubRepository:        pubSubRepository,
		gcsRepository:           gcsRepository,
		pubSubMessgaeDescriptor: make(map[string]protoreflect.MessageDescriptor),
		pubSubTopic:             make(map[string]string),
		offsetBucketName:        serverConfig.OffsetBucketName,
		offsetObjectPrefix:      serverConfig.OffsetObjectPrefix,
		err:                     nil,
	}

	for i := 0; i < len(u.serverConfig.SourceTables); i++ {
		u.GenerateProtoMessageDescriptor(u.serverConfig.PubSubSchemaIds[i], u.serverConfig.SourceTables[i])
		u.pubSubTopic[u.serverConfig.SourceTables[i]] = u.serverConfig.PubSubTopicIds[i]
	}

	return u, nil

}

func (u *usecase) String() string { return "usecase" }
