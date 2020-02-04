package policy

import (
	"reflect"
	"testing"
)

func TestErrorHandlingPolicyMerge(t *testing.T) {
	type fields struct {
		IgnoreFileErrors         bool
		IgnoreFileErrorsSet      bool
		IgnoreDirectoryErrors    bool
		IgnoreDirectoryErrorsSet bool
	}
	type args struct {
		src ErrorHandlingPolicy
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		expResult ErrorHandlingPolicy
	}{
		{
			name: "Policy being merged has no value set - expect no change",
			fields: fields{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:         false,
					IgnoreFileErrorsSet:      false,
					IgnoreDirectoryErrors:    false,
					IgnoreDirectoryErrorsSet: false,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
		},
		{
			name: "Policy being merged has a true value but it wasn't actually set - expect no change",
			fields: fields{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:         true,
					IgnoreFileErrorsSet:      false,
					IgnoreDirectoryErrors:    false,
					IgnoreDirectoryErrorsSet: false,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
		},
		{
			name: "Starting policy has a true value but not set - expect no change",
			fields: fields{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:         false,
					IgnoreFileErrorsSet:      false,
					IgnoreDirectoryErrors:    false,
					IgnoreDirectoryErrorsSet: false,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
		},
		{
			name: "Policy being merged has a value set at false - expect result to have value set at false",
			fields: fields{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:         false,
					IgnoreFileErrorsSet:      true,
					IgnoreDirectoryErrors:    false,
					IgnoreDirectoryErrorsSet: false,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
		},
		{
			name: "Policy being merged has a value set at true - expect result to have value set at true",
			fields: fields{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:         true,
					IgnoreFileErrorsSet:      true,
					IgnoreDirectoryErrors:    false,
					IgnoreDirectoryErrorsSet: false,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
		},
		{
			name: "Starting policy already has a value set at false - expect no change from merged policy",
			fields: fields{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:         true,
					IgnoreFileErrorsSet:      true,
					IgnoreDirectoryErrors:    false,
					IgnoreDirectoryErrorsSet: false,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
		},
		{
			name: "Policy being merged has a value set at true - expect no change from merged policy",
			fields: fields{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:         false,
					IgnoreFileErrorsSet:      true,
					IgnoreDirectoryErrors:    false,
					IgnoreDirectoryErrorsSet: false,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
		},
		{
			name: "Both error behavior changed at once",
			fields: fields{
				IgnoreFileErrors:         false,
				IgnoreFileErrorsSet:      false,
				IgnoreDirectoryErrors:    false,
				IgnoreDirectoryErrorsSet: false,
			},
			args: args{
				src: ErrorHandlingPolicy{
					IgnoreFileErrors:         true,
					IgnoreFileErrorsSet:      true,
					IgnoreDirectoryErrors:    true,
					IgnoreDirectoryErrorsSet: true,
				},
			},
			expResult: ErrorHandlingPolicy{
				IgnoreFileErrors:         true,
				IgnoreFileErrorsSet:      true,
				IgnoreDirectoryErrors:    true,
				IgnoreDirectoryErrorsSet: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ErrorHandlingPolicy{
				IgnoreFileErrors:         tt.fields.IgnoreFileErrors,
				IgnoreFileErrorsSet:      tt.fields.IgnoreFileErrorsSet,
				IgnoreDirectoryErrors:    tt.fields.IgnoreDirectoryErrors,
				IgnoreDirectoryErrorsSet: tt.fields.IgnoreDirectoryErrorsSet,
			}
			p.Merge(tt.args.src)

			if !reflect.DeepEqual(*p, tt.expResult) {
				t.Errorf("Policy after merge was not what was expected\n%v != %v", p, tt.expResult)
			}
		})
	}
}
