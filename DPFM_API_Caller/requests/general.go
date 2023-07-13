package requests

type General struct {
	Equipment              	int		`json:"Equipment"`
	IsMarkedForDeletion		*bool   `json:"IsMarkedForDeletion"`
}
