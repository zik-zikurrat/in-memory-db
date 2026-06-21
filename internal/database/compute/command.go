package compute

const (
	// COMMANDS
	SetCommand = "SET"
	GetCommand = "GET"
	DelCommand = "DEL"
)

var commandArity = map[string]int{
	SetCommand: 3,
	GetCommand: 1,
	DelCommand: 1,
}
