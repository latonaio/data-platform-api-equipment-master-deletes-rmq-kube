package dpfm_api_output_formatter

import (
	"database/sql"
	"fmt"
)

func ConvertToGeneral(rows *sql.Rows) (*General, error) {
	defer rows.Close()
	general := General{}
	i := 0

	for rows.Next() {
		i++
		err := rows.Scan(
			&general.Equipment,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &general, err
		}

	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &general, nil
	}

	return &general, nil
}

func ConvertToOwnerBusinessPartner(rows *sql.Rows) (*[]OwnerBusinessPartner, error) {
	defer rows.Close()
	ownerBusinessPartners := make([]OwnerBusinessPartner, 0)
	i := 0

	for rows.Next() {
		i++
		ownerBusinessPartner := OwnerBusinessPartner{}
		err := rows.Scan(
			&ownerBusinessPartner.Equipment,
			&ownerBusinessPartner.OwnerBusinessPartner,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &ownerBusinessPartners, err
		}

		ownerBusinessPartners = append(ownerBusinessPartners, ownerBusinessPartner)
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &ownerBusinessPartners, nil
	}

	return &ownerBusinessPartners, nil
}
