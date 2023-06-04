package repository

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"excalibur-platform/stream-cdc-mysql/domain"

	"cloud.google.com/go/storage"
	"github.com/siddontang/go-log/log"
)

type gcsRepository struct {
	Ctx       context.Context
	gcsClient *storage.Client
}

func NewGCSRepository(
	gcsClient *storage.Client,
) domain.GCSRepository {

	return &gcsRepository{
		Ctx:       context.Background(),
		gcsClient: gcsClient,
	}

}

func (repo *gcsRepository) DownloadObject(bucketName string, objectName string) ([]byte, error) {

	var err error
	var rc *storage.Reader

	rc, err = repo.gcsClient.Bucket(bucketName).Object(objectName).NewReader(repo.Ctx)

	if err != nil {
		log.Errorf("[GCS Repository][Download Object] Err : %s\n", err.Error())
		return nil, err
	}

	var data []byte

	data, err = ioutil.ReadAll(rc)

	if err != nil {
		log.Errorf("[GCS Repository][Download Object] Err : %s\n", err.Error())
		return nil, err
	}

	err = rc.Close()

	if err != nil {
		log.Errorf("[GCS Repository][Download Object] Err : %s\n", err.Error())
		return nil, err
	}

	return data, nil

}

func (repo *gcsRepository) UploadObject(bucketName string, objectName string, data []byte) error {

	var err error
	var wc *storage.Writer = repo.gcsClient.Bucket(bucketName).Object(objectName).NewWriter(repo.Ctx)

	buf := bytes.NewBuffer(data)

	_, err = io.Copy(wc, buf)

	if err != nil {
		log.Errorf("[GCS Repository][Upload Object] Err : %s\n", err.Error())
		return err
	}

	err = wc.Close()

	if err != nil {
		log.Errorf("[GCS Repository][Upload Object] Err : %s\n", err.Error())
		return err
	}

	return nil

}
