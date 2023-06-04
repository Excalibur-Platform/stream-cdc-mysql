package usecase

import (
	"encoding/json"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

func (u *usecase) SetGTIDOffsetJob(gtid mysql.GTIDSet, force bool) {

	if u.lastGTIDOffsetStoredTs != nil {

		if time.Since(*u.lastGTIDOffsetStoredTs).Minutes() < 5.0 {
			return
		}

	}

	u.gtidMx.Lock()
	defer u.gtidMx.Unlock()

	var err error
	var data []byte

	data, err = json.Marshal(gtid)

	if err != nil {
		log.Errorf("[Usecase][On Pos Synced] Err : %s\n", err.Error())
		u.errMx.Lock()
		u.err = err
		u.errMx.Unlock()
		return
	}

	err = u.gcsRepository.UploadObject(
		u.offsetBucketName,
		u.offsetObjectPrefix+"_gtid.json",
		data,
	)

	if err != nil {
		u.errMx.Lock()
		u.err = err
		u.errMx.Unlock()
		return
	}

	var nowTime time.Time = time.Now()
	u.lastGTIDOffsetStoredTs = &nowTime

}
