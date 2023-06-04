package usecase

import (
	"encoding/json"
	"time"

	"excalibur-platform/stream-cdc-mysql/domain"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/dynamicpb"
)

func (u *usecase) OnRowJob(e canal.RowsEvent) {

	var eventTimestamp domain.JSONTime = domain.JSONTime(time.Now())

	var row domain.DataEvent = domain.DataEvent{
		Action:         &e.Action,
		EventTimestamp: &eventTimestamp,
	}

	if e.Header != nil {
		var repEventTimestamp domain.JSONTime = domain.JSONTime(time.Unix(int64(e.Header.Timestamp), 0))
		row.ReplicationEventTimestamp = &repEventTimestamp
	}

	for i := 0; i < len(e.Rows); i++ {

		var data map[string]interface{} = make(map[string]interface{})

		for j := 0; j < len(e.Table.Columns); j++ {

			enumIdx, ok := e.Rows[i][j].(int64)

			if e.Table.Columns[j].Type == schema.TYPE_ENUM && ok {
				if enumIdx > 0 {
					data[e.Table.Columns[j].Name] = e.Table.Columns[j].EnumValues[enumIdx-1]
				} else {
					data[e.Table.Columns[j].Name] = nil
				}

			} else {
				data[e.Table.Columns[j].Name] = e.Rows[i][j]
			}

		}

		if i == 0 {

			if e.Action == canal.InsertAction {
				row.After = data
			} else if e.Action == canal.UpdateAction || e.Action == canal.DeleteAction {
				row.Before = data
			}

		} else if i == 1 && e.Action == canal.UpdateAction {
			row.After = data
		}

	}

	json_data, err := json.Marshal(row)

	if err != nil {
		log.Errorf("[Usecase][On Row] Err : %s | %s\n", err.Error(), json_data)
		u.errMx.Lock()
		u.err = err
		u.errMx.Unlock()
		return
	}

	dp := dynamicpb.NewMessageType(u.pubSubMessgaeDescriptor[e.Table.Name])
	mi := dp.New().Interface()

	err = protojson.Unmarshal(json_data, mi)

	if err != nil {
		log.Errorf("[Usecase][On Row] Err : %s | %s\n", err.Error(), json_data)
		u.errMx.Lock()
		u.err = err
		u.errMx.Unlock()
		return
	}

	err = u.pubSubRepository.PublishMessage(u.pubSubTopic[e.Table.Name], mi)

	if err != nil {
		u.errMx.Lock()
		u.err = err
		u.errMx.Unlock()
		return
	}

}
