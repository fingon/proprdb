package proprdbrt

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"gotest.tools/v3/assert"
)

func TestProjectScalarPath(t *testing.T) {
	cases := []struct {
		name      string
		message   proto.Message
		path      []protoreflect.FieldNumber
		expected  any
		errorText string
	}{
		{"bool", wrapperspb.Bool(false), []protoreflect.FieldNumber{1}, false, ""},
		{"int32", wrapperspb.Int32(-4), []protoreflect.FieldNumber{1}, int64(-4), ""},
		{"int64", wrapperspb.Int64(42), []protoreflect.FieldNumber{1}, int64(42), ""},
		{"uint32", wrapperspb.UInt32(42), []protoreflect.FieldNumber{1}, int64(42), ""},
		{"float", wrapperspb.Float(0), []protoreflect.FieldNumber{1}, float64(0), ""},
		{"double", wrapperspb.Double(-5), []protoreflect.FieldNumber{1}, float64(-5), ""},
		{"string", wrapperspb.String(""), []protoreflect.FieldNumber{1}, "", ""},
		{"bytes", wrapperspb.Bytes(nil), []protoreflect.FieldNumber{1}, []byte{}, ""},
		{"enum", structpb.NewNullValue(), []protoreflect.FieldNumber{1}, int64(0), ""},
		{"absent oneof", structpb.NewStringValue(""), []protoreflect.FieldNumber{1}, nil, ""},
		{"typed nil", (*wrapperspb.BoolValue)(nil), []protoreflect.FieldNumber{1}, nil, "valid message"},
		{"list", &structpb.ListValue{}, []protoreflect.FieldNumber{1}, nil, "must be singular"},
		{"map", &structpb.Struct{}, []protoreflect.FieldNumber{1}, nil, "must be singular"},
		{"nil", nil, []protoreflect.FieldNumber{1}, nil, "requires a message"},
		{"empty", wrapperspb.Bool(false), nil, nil, "requires a message"},
		{"unknown", wrapperspb.Bool(false), []protoreflect.FieldNumber{2}, nil, "missing"},
		{"scalar intermediate", wrapperspb.Bool(false), []protoreflect.FieldNumber{1, 1}, nil, "must be a message"},
		{"unsupported", wrapperspb.UInt64(1), []protoreflect.FieldNumber{1}, nil, "unsupported"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, err := ProjectScalarPath(tc.message, tc.path)
			if tc.errorText != "" {
				assert.ErrorContains(t, err, tc.errorText)
				return
			}
			assert.NilError(t, err)
			assert.DeepEqual(t, value, tc.expected)
		})
	}
}
