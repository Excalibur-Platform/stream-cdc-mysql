package usecase

import (
	"encoding/json"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

func (u *usecase) GetLastGTIDStored() (mysql.GTIDSet, error) {

	var err error
	var data []byte

	data, err = u.gcsRepository.DownloadObject(
		u.offsetBucketName,
		u.offsetObjectPrefix+"_gtid.json",
	)

	if err != nil {
		return nil, err
	}

	var gtidset mysql.GTIDSet

	if u.serverConfig.SourceType == mysql.MySQLFlavor {

		gtidset = new(mysql.MysqlGTIDSet)
		err = json.Unmarshal(data, gtidset)

	} else if u.serverConfig.SourceType == mysql.MariaDBFlavor {

		gtidset = new(mysql.MariadbGTIDSet)
		err = json.Unmarshal(data, gtidset)

	}

	if err != nil {
		log.Errorf("[Usecase][Get Last Gtid Pos Stored] Err : %s\n", err.Error())
		return nil, err
	}

	return gtidset, nil

}
