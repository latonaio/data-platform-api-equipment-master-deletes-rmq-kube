package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-equipment-master-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-equipment-master-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"data-platform-api-equipment-master-deletes-rmq-kube/config"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	database "github.com/latonaio/golang-mysql-network-connector"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client-for-data-platform"
	"golang.org/x/xerrors"
)

type DPFMAPICaller struct {
	ctx  context.Context
	conf *config.Conf
	rmq  *rabbitmq.RabbitmqClient
	db   *database.Mysql
}

func NewDPFMAPICaller(
	conf *config.Conf, rmq *rabbitmq.RabbitmqClient, db *database.Mysql,
) *DPFMAPICaller {
	return &DPFMAPICaller{
		ctx:  context.Background(),
		conf: conf,
		rmq:  rmq,
		db:   db,
	}
}

func (c *DPFMAPICaller) AsyncDeletes(
	accepter []string,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (interface{}, []error) {
	var response interface{}
	if input.APIType == "deletes" {
		response = c.deleteSqlProcess(input, output, accepter, log)
	}

	return response, nil
}

func (c *DPFMAPICaller) deleteSqlProcess(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	log *logger.Logger,
) *dpfm_api_output_formatter.Message {
	var generalData *dpfm_api_output_formatter.General
	ownerBusinessPartnerData := make([]dpfm_api_output_formatter.OwnerBusinessPartner, 0)
	for _, a := range accepter {
		switch a {
		case "General":
			h, i := c.generalDelete(input, output, log)
			generalData = h
			if h == nil || i == nil {
				continue
			}
			ownerBusinessPartnerData = append(ownerBusinessPartnerData, *i...)
		case "OwnerBusinessPartner":
			i := c.ownerBusinessPartnerDelete(input, output, log)
			if i == nil {
				continue
			}
			ownerBusinessPartnerData = append(ownerBusinessPartnerData, *i...)
		}
	}

	return &dpfm_api_output_formatter.Message{
		General					: generalData,
		OwnerBusinessPartner	: &ownerBusinessPartnerData,
	}
}

func (c *DPFMAPICaller) generalDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) (*dpfm_api_output_formatter.General, *[]dpfm_api_output_formatter.OwnerBusinessPartner) {
	sessionID := input.RuntimeSessionID
	general := c.GeneralRead(input, log)
	general.Equipment = input.General.Equipment
	general.IsMarkedForDeletion = input.General.IsMarkedForDeletion
	res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "EquipmentMasterGeneral", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		log.Error("%+v", err)
		return nil, nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "General Data cannot delete"
		return nil, nil
	}

	// generalの削除が取り消された時は子に影響を与えない
	if !*general.IsMarkedForDeletion {
		return general, nil
	}

	ownerBusinessPartners := c.OwnerBusinessPartnersRead(input, log)
	for i := range *ownerBusinessPartners {
		(*ownerBusinessPartners)[i].IsMarkedForDeletion = input.General.IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": (*ownerBusinessPartners)[i], "function": "EquipmentMasterOwnerBusinessPartner", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil, nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "OwnerBusinessPartner Data cannot delete"
			return nil, nil
		}
	}

	return general, ownerBusinessPartners
}

func (c *DPFMAPICaller) ownerBusinessPartnerDelete(
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.OwnerBusinessPartner {
	sessionID := input.RuntimeSessionID

	ownerBusinessPartners := make([]dpfm_api_output_formatter.OwnerBusinessPartner, 0)
	for _, v := range input.General.OwnerBusinessPartner {
		data := dpfm_api_output_formatter.OwnerBusinessPartner{
			Equipment:		     		input.General.Equipment,
			OwnerBusinessPartner:		v.OwnerBusinessPartner,
			IsMarkedForDeletion:		v.IsMarkedForDeletion,
		}
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{
			"message":            data,
			"function":           "EquipmentMasterOwnerBusinessPartner",
			"runtime_session_id": sessionID,
		})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "OwnerBusinessPartner Data cannot delete"
			return nil
		}
	}
	// ownerBusinessPartnerがキャンセル取り消しされた場合、generalのキャンセルも取り消す
	if !*input.General.OwnerBusinessPartner[0].IsMarkedForDeletion {
		general := c.GeneralRead(input, log)
		general.IsMarkedForDeletion = input.General.OwnerBusinessPartner[0].IsMarkedForDeletion
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": general, "function": "EquipmentMasterGeneral", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			log.Error("%+v", err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "General Data cannot delete"
			return nil
		}
	}

	return &ownerBusinessPartners
}

func checkResult(msg rabbitmq.RabbitmqMessage) bool {
	data := msg.Data()
	d, ok := data["result"]
	if !ok {
		return false
	}
	result, ok := d.(string)
	if !ok {
		return false
	}
	return result == "success"
}

func getBoolPtr(b bool) *bool {
	return &b
}
