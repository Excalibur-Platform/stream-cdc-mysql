package usecase

import (
	"encoding/json"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

func (u *usecase) SetBinLogPosOffsetJob(pos mysql.Position, force bool) {

	if u.lastBinLogPosOffsetStoredTs != nil {

		if time.Since(*u.lastBinLogPosOffsetStoredTs).Minutes() < 5.0 {
			return
		}

	}

	u.binLogMX.Lock()
	defer u.binLogMX.Unlock()

	var err error
	var data []byte

	data, err = json.Marshal(pos)

	if err != nil {
		log.Errorf("[Usecase][On Pos Synced] Err : %s\n", err.Error())
		u.errMx.Lock()
		u.err = err
		u.errMx.Unlock()
		return
	}

	err = u.gcsRepository.UploadObject(
		u.offsetBucketName,
		u.offsetObjectPrefix+"_binlog_pos.json",
		data,
	)

	if err != nil {
		u.errMx.Lock()
		u.err = err
		u.errMx.Unlock()
		return
	}

	var nowTime time.Time = time.Now()
	u.lastBinLogPosOffsetStoredTs = &nowTime

}
