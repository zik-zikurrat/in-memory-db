package compute

const (
	// COMMANDS
	SetCommand  = "SET"
	GetCommand  = "GET"
	DelCommand  = "DEL"
	ScanCommand = "SCAN"
	KeysCommand = "KEYS"
)

var commandArity = map[string]int{
	SetCommand:  4,
	GetCommand:  1,
	DelCommand:  1,
	ScanCommand: 4,
	KeysCommand: 2,
}
