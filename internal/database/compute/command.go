package compute

const (
	SetCommand = "SET"
	GetCommand = "GET"
	DelCommand = "DEL"
)

var commandArity = map[string]int{
	SetCommand: 2,
	GetCommand: 1,
	DelCommand: 1,
}
