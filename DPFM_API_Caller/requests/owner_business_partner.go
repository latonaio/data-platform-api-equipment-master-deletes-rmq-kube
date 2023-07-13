package requests

type OwnerBusinessPartner struct {
	Equipment              	int		`json:"Equipment"`
	OwnerBusinessPartner    int     `json:"OwnerBusinessPartner"`
	IsMarkedForDeletion		*bool   `json:"IsMarkedForDeletion"`
}
