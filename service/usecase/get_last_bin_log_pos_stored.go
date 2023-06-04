package usecase

import (
	"encoding/json"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

func (u *usecase) GetLastBinLogPosStored() (*mysql.Position, error) {

	var err error
	var data []byte

	data, err = u.gcsRepository.DownloadObject(
		u.offsetBucketName,
		u.offsetObjectPrefix+"_binlog_pos.json",
	)

	if err != nil {
		return nil, err
	}

	var pos *mysql.Position = &mysql.Position{}

	err = json.Unmarshal(data, pos)

	if err != nil {
		log.Errorf("[Usecase][Get Last BinLog Pos Stored] Err : %s\n", err.Error())
		return nil, err
	}

	if pos != nil && pos.Name == "" {
		return nil, fmt.Errorf("pos not found")
	}

	return pos, nil

}
