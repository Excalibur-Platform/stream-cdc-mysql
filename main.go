package main

import (
	"runtime"

	"excalibur-platform/stream-cdc-mysql/domain"
	"excalibur-platform/stream-cdc-mysql/service/repository"
	"excalibur-platform/stream-cdc-mysql/service/usecase"
	"excalibur-platform/stream-cdc-mysql/utils"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/siddontang/go-log/log"
)

func run(server *utils.Server) error {

	log.Infof("[RUN] Initiate Runner")

	var err error

	var pubsubRepository domain.PubSubRepository
	var gcsRepository domain.GCSRepository

	pubsubRepository = repository.NewPubSubRepository(
		server.PubsubClient,
		server.PubsubSchemaClient,
	)

	gcsRepository = repository.NewGCSRepository(
		server.GcsClient,
	)

	var u domain.Usecase

	u, err = usecase.NewUsecase(
		*server.Config,
		pubsubRepository,
		gcsRepository,
	)

	if err != nil {
		return err
	}

	log.Infof("[RUN] Set Handler")

	server.Canal.SetEventHandler(u)

	log.Infof("[RUN] Check Existing GTIDSet")

	var gtidset mysql.GTIDSet

	gtidset, err = u.GetLastGTIDStored()

	if err == nil {
		log.Infof("[RUN] Running from exisiting gtidset")

		err = server.Canal.StartFromGTID(gtidset)

		if err == nil {
			return nil
		}
	}

	var pos *mysql.Position

	log.Infof("[RUN] Check Existing Offset")

	pos, err = u.GetLastBinLogPosStored()

	if err == nil && pos != nil {
		log.Infof("[RUN] Run with Exisiting BinLog Position")

		err = server.Canal.RunFrom(*pos)

		if err == nil {
			return nil
		}
	}

	log.Infof("[RUN] Checking Master GTID")

	var masterGTID mysql.GTIDSet

	masterGTID, err = server.Canal.GetMasterGTIDSet()

	if err == nil {

		log.Infof("[RUN] Run with Master GTID")

		err = server.Canal.StartFromGTID(masterGTID)

		if err == nil {
			return nil
		}

	}

	log.Infof("[RUN] Checking Master Binlog")

	var masterPos mysql.Position

	masterPos, err = server.Canal.GetMasterPos()

	if err == nil {

		log.Infof("[RUN] Run with Master BinLog")

		err = server.Canal.RunFrom(masterPos)

		if err == nil {
			return nil
		}

	}

	return err

}

func main() {

	server, err := utils.NewServer()

	if err != nil {
		log.Fatal(err)
	}

	runtime.GOMAXPROCS(server.Config.ThreadNumber)

	log.Infoln(server.Config)

	defer server.Close()
	err = run(server)

	if err != nil {
		log.Fatal(err)
	}

}
