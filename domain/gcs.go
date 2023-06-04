package domain

type GCSRepository interface {
	DownloadObject(bucketName string, objectName string) ([]byte, error)
	UploadObject(bucketName string, objectName string, data []byte) error
}
