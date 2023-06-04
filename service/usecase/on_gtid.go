package usecase

import "github.com/go-mysql-org/go-mysql/mysql"

func (u *usecase) OnGTID(gtid mysql.GTIDSet) error {

	go u.SetGTIDOffsetJob(gtid, false)

	if u.err != nil {
		return u.err
	}

	return nil

}
