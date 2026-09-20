package compute

const (
	// COMMANDS
	SetCommand  = "SET"
	GetCommand  = "GET"
	DelCommand  = "DEL"
	ScanCommand = "SCAN"
	KeysCommand = "KEYS"
)

type arity struct {
	min int
	max int
}

var commandArity = map[string]arity{
	SetCommand:  {min: 2, max: 3}, // key value [ttl]
	GetCommand:  {min: 1, max: 1}, // key
	DelCommand:  {min: 1, max: 1}, // key
	ScanCommand: {min: 1, max: 1}, // cursor
	KeysCommand: {min: 0, max: 1}, // [pattern], по умолчанию "*"
}
