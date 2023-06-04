package usecase

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

func (u *usecase) OnRow(e *canal.RowsEvent) error {

	if _, ok := u.pubSubMessgaeDescriptor[e.Table.Name]; !ok {
		return nil
	}

	if e == nil {
		return nil
	}

	go u.OnRowJob(*e)

	if u.err != nil {
		return u.err
	}

	return nil

}
