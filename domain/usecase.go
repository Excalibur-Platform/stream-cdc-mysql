package domain

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type Usecase interface {
	canal.EventHandler
	OnRowJob(e canal.RowsEvent)
	SetBinLogPosOffsetJob(pos mysql.Position, force bool)
	SetGTIDOffsetJob(gtid mysql.GTIDSet, force bool)
	GetLastGTIDStored() (mysql.GTIDSet, error)
	GetLastBinLogPosStored() (*mysql.Position, error)
}
