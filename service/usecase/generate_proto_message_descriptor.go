package usecase

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/siddontang/go-log/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func (u *usecase) GenerateProtoMessageDescriptor(schemaId string, tableName string) error {

	var err error

	var protoFile *string

	protoFile, err = u.pubSubRepository.GetSchema(schemaId)

	if err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	var f *os.File

	f, err = os.OpenFile("./schema.proto", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	_, err = f.WriteString(*protoFile)

	if err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	err = f.Close()

	if err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	cmd := exec.Command(
		"protoc",
		"--include_source_info",
		"--descriptor_set_out=schema.proto.pb",
		"schema.proto",
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()

	if err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	os.Remove("./schema.proto")

	var protoDescFile []byte

	protoDescFile, err = ioutil.ReadFile("./schema.proto.pb")

	if err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	os.Remove("./schema.proto.pb")

	pbSet := new(descriptorpb.FileDescriptorSet)

	if err := proto.Unmarshal(protoDescFile, pbSet); err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	ff, err := protodesc.NewFiles(pbSet)

	if err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	d, err := ff.FindDescriptorByName(protoreflect.FullName("Data"))

	if err != nil {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", err.Error())
		return err
	}

	md, ok := d.(protoreflect.MessageDescriptor)

	if !ok {
		log.Errorf("[Usecase][Generate Proto Message Descriptor] Err : %s\n", "MessageDecsriptor Not Found")
		return fmt.Errorf("MessageDecsriptor Not Found")
	}

	u.pubSubMessgaeDescriptor[tableName] = md

	return nil

}
