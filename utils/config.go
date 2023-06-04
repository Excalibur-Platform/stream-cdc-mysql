package utils

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Config struct {
	PubSubProjectID    string
	SourceType         string
	SourceHost         string
	SourcePort         string
	SourceUser         string
	SourcePassword     string
	SourceDatabaseName string
	SourceTables       []string
	PubSubSchemaIds    []string
	PubSubTopicIds     []string
	OffsetBucketName   string
	OffsetObjectPrefix string
	MaxAllowedPacketMB int
	ThreadNumber       int
}

func Getenv(key, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return defaultValue
	}
	return value
}

func NewConfig() (*Config, error) {

	var err error

	var cfg *Config = &Config{}

	cfg.PubSubProjectID = Getenv("PUBSUB_PROJECT_ID", "trial-pusdatin-kemenkes")
	cfg.SourceType = Getenv("SOURCE_TYPE", "mariadb")
	cfg.SourceHost = Getenv("SOURCE_HOST", "127.0.0.1")
	cfg.SourcePort = Getenv("SOURCE_PORT", "3306")
	cfg.SourceUser = Getenv("SOURCE_USER", "root")
	cfg.SourcePassword = Getenv("SOURCE_PASSWORD", "admin")
	cfg.SourceDatabaseName = Getenv("SOURCE_DATABASE_NAME", "sisdmk-dikti")
	cfg.OffsetBucketName = Getenv("OFFSET_BUCKET_NAME", "dto-ndjson-datalake-storage-test")
	cfg.OffsetObjectPrefix = Getenv("OFFSET_OBJECT_PREFIX", "stream_offset/bronze_stream_nar_antigen.dt_antigen_sampel")

	var maxAllowedPacketMB string = Getenv("MAX_ALLOWED_PACKET_MB", "500")
	cfg.MaxAllowedPacketMB, err = strconv.Atoi(maxAllowedPacketMB)

	if err != nil {
		return nil, err
	}

	var threadNumber string = Getenv("THREAD_NUMBER", "1")

	cfg.ThreadNumber, err = strconv.Atoi(threadNumber)

	if err != nil {
		return nil, err
	}

	var sourceTables string = Getenv("SOURCE_TABLES", "dummy_table")
	cfg.SourceTables = strings.Split(sourceTables, ",")

	var pubSubSchemaIDS string = Getenv("PUBSUB_SCHEMA_IDS", "bronze_stream_nar_antigen.dt_antigen_sampel")
	cfg.PubSubSchemaIds = strings.Split(pubSubSchemaIDS, ",")

	var pubSubTopicIDS string = Getenv("PUBSUB_TOPIC_IDS", "bronze_stream_nar_antigen.dt_antigen_sampel")
	cfg.PubSubTopicIds = strings.Split(pubSubTopicIDS, ",")

	count := []int{len(cfg.SourceTables), len(cfg.PubSubSchemaIds), len(cfg.PubSubTopicIds)}

	sort.Slice(count, func(i, j int) bool {
		return count[i] < count[j]
	})

	var isDiff bool = false
	var prefVal int = -1

	for i := 0; i < len(count); i++ {

		if prefVal == -1 {
			prefVal = count[i]
		}

		if count[i] != prefVal {
			isDiff = true
			break
		}

	}

	if isDiff {
		return nil, fmt.Errorf("length source_tables, pubsub_schema_ids, pubsub_topic_ids must all same")
	}

	return cfg, nil

}
