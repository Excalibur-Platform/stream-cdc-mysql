package usecase

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

func (u *usecase) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {

	u.SetGTIDOffsetJob(set, true)
	u.SetBinLogPosOffsetJob(pos, true)

	if u.err != nil {
		return u.err
	}

	return nil

}
