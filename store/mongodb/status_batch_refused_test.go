package mongodb

import (
	"errors"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// refusedCommand must recognise a rejected bulkWrite in both shapes the
// driver returns it (MongoDB 7 parses and rejects the command; an 8.0 binary
// at feature compatibility version 7.0 refuses it as unsupported), and must
// never treat a failure after operations ran as a refusal: falling back then
// would re-apply rows that were written.
func TestRefusedCommand(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"7.0: unknown field", mongo.CommandError{Code: codeIDLUnknownField, Name: "IDLUnknownField"}, true},
		{"unknown command", mongo.CommandError{Code: codeCommandNotFound}, true},
		{"8.0 at FCV 7.0", mongo.ClientBulkWriteException{WriteError: &mongo.WriteError{Code: codeCommandNotSupported}}, true},
		{"wrapped", fmt.Errorf("bulk: %w", mongo.CommandError{Code: codeCommandNotSupported}), true},
		{"refusal code but results came back", mongo.ClientBulkWriteException{
			WriteError: &mongo.WriteError{Code: codeCommandNotSupported}, PartialResult: &mongo.ClientBulkWriteResult{},
		}, false},
		{"refusal code alongside write errors", mongo.ClientBulkWriteException{
			WriteError:  &mongo.WriteError{Code: codeCommandNotSupported},
			WriteErrors: map[int]mongo.WriteError{0: {Code: 11000}},
		}, false},
		{"write concern failure", mongo.ClientBulkWriteException{WriteConcernErrors: []mongo.WriteConcernError{{Code: 64}}}, false},
		{"other command error", mongo.CommandError{Code: 11600, Name: "InterruptedAtShutdown"}, false},
		{"transport error", errors.New("connection reset"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := refusedCommand(tc.err); got != tc.want {
				t.Errorf("refusedCommand(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
