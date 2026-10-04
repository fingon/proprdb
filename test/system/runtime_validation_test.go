package genexample

import (
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

const (
	validationUUIDv7  = "018f4f3f-6f9f-7a1b-8f55-1234567890ab"
	validationTypeURL = "type.googleapis.com/generatedtest.example.Person"
)

func TestValidateUUIDv7(t *testing.T) {
	testCases := []struct {
		name  string
		id    string
		valid bool
	}{
		{name: "valid", id: validationUUIDv7, valid: true},
		{name: "wrong version", id: "018f4f3f-6f9f-4a1b-8f55-1234567890ab"},
		{name: "wrong variant", id: "018f4f3f-6f9f-7a1b-7f55-1234567890ab"},
		{name: "uppercase", id: "018F4F3F-6F9F-7A1B-8F55-1234567890AB"},
		{name: "malformed", id: testInvalidUUID},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := rt.ValidateUUIDv7(testCase.id)
			assert.Check(t, is.Equal(err == nil, testCase.valid))
		})
	}
}

func TestValidateUUIDVersions(t *testing.T) {
	for version := byte('1'); version <= '8'; version++ {
		id := validationUUIDv7[:14] + string(version) + validationUUIDv7[15:]
		t.Run(string(version), func(t *testing.T) { assert.NilError(t, rt.ValidateUUID(id)) })
	}
	for _, id := range []string{
		"", testInvalidUUID, strings.ToUpper(validationUUIDv7),
		"018f4f3f-6f9f-0a1b-8f55-1234567890ab",
		"018f4f3f-6f9f-9a1b-8f55-1234567890ab",
		"018f4f3f-6f9f-7a1b-7f55-1234567890ab",
	} {
		t.Run(id, func(t *testing.T) { assert.Check(t, rt.ValidateUUID(id) != nil) })
	}
}

func TestReadJSONLConformance(t *testing.T) {
	validData := `{"@type":"` + validationTypeURL + `","name":"` + testPersonNameAda + `"}`
	validLine := `{"id":"` + validationUUIDv7 + `","atNs":"42","data":` + validData + `}`
	testCases := []struct {
		name  string
		input string
		valid bool
	}{
		{name: "valid", input: validLine, valid: true},
		{name: "UUIDv4", input: strings.Replace(validLine, validationUUIDv7, validationUUIDv4, 1), valid: true},
		{name: "UUIDv5", input: strings.Replace(validLine, validationUUIDv7, validationUUIDv5, 1), valid: true},
		{name: "blank line", input: "\n" + validLine + "\r\n", valid: true},
		{name: "multiline object", input: "{\n" + validLine[1:]},
		{name: "multiple values", input: validLine + " " + validLine},
		{name: "integer deleted", input: strings.Replace(validLine, `"atNs"`, `"deleted":1,"atNs"`, 1)},
		{name: "null deleted", input: strings.Replace(validLine, `"atNs"`, `"deleted":null,"atNs"`, 1)},
		{name: "boolean timestamp", input: strings.Replace(validLine, `"42"`, "true", 1)},
		{name: "fraction timestamp", input: strings.Replace(validLine, `"42"`, "42.5", 1)},
		{name: "exponent timestamp", input: strings.Replace(validLine, `"42"`, "42e0", 1)},
		{name: "array data", input: strings.Replace(validLine, validData, "[]", 1)},
		{name: "missing type", input: strings.Replace(validLine, validData, `{}`, 1)},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			visits := 0
			err := rt.ReadJSONL(strings.NewReader(testCase.input), func(_ rt.JSONLRecord, _ int) error {
				visits++
				return nil
			})
			assert.Check(t, is.Equal(err == nil, testCase.valid))
			if testCase.valid {
				assert.Check(t, is.Equal(visits, 1))
			}
		})
	}
}
