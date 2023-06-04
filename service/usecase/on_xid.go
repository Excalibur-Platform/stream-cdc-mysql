package usecase

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

func (u *usecase) OnXID(nextPos mysql.Position) error {

	go u.SetBinLogPosOffsetJob(nextPos, false)

	if u.err != nil {
		return u.err
	}

	return nil

}
