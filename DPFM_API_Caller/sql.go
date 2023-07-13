package dpfm_api_caller

import (
	dpfm_api_input_reader "data-platform-api-equipment-master-deletes-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-equipment-master-deletes-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	"strings"
)

func (c *DPFMAPICaller) GeneralRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *dpfm_api_output_formatter.General {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE general.Equipment = \"%s\" ", input.General.Equipment),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	general.Equipment
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_equipment_master_general_data as general 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGeneral(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) OwnerBusinessPartnersRead(
	input *dpfm_api_input_reader.SDC,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.OwnerBusinessPartner {
	where := strings.Join([]string{
		fmt.Sprintf("WHERE ownerBusinessPartner.Equipment = %d ", input.General.Equipment),
		fmt.Sprintf("AND ownerBusinessPartner.OwnerBusinessPartner = %d ", input.General.OwnerBusinessPartner[0].OwnerBusinessPartner),
		fmt.Sprintf("AND validityStartDate = \"%s\" ", input.General.OwnerBusinessPartner[0].ValidityStartDate),
		fmt.Sprintf("AND validityEndDate = \"%s\" ", input.General.OwnerBusinessPartner[0].ValidityEndDate),
	}, "")

	rows, err := c.db.Query(
		`SELECT 
    	ownerBusinessPartner.Equipment,
    	ownerBusinessPartner.OwnerBusinessPartner,
    	ownerBusinessPartner.ValidityStartDate,
    	ownerBusinessPartner.ValidityEndDate
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_equipment_master_owner_business_partner_data as businessPartner 
		` + where + ` ;`)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToOwnerBusinessPartner(rows)
	if err != nil {
		log.Error("%+v", err)
		return nil
	}

	return data
}
