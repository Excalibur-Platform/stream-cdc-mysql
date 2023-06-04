package domain

type DataEvent struct {
	Action                    *string                `json:"actions"`
	Before                    map[string]interface{} `json:"before"`
	After                     map[string]interface{} `json:"after"`
	ReplicationEventTimestamp *JSONTime              `json:"replication_event_timestamp"`
	EventTimestamp            *JSONTime              `json:"event_timestamp"`
}
