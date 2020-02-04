package policy

// ErrorHandlingPolicy controls error hadnling behavior when taking snapshots.
type ErrorHandlingPolicy struct {
	IgnoreFileErrors    bool `json:"ignoreFileErrs,omitempty"`
	IgnoreFileErrorsSet bool `json:"ignoreFileErrsSet,omitempty"`

	IgnoreDirectoryErrors    bool `json:"ignoreDirErrs,omitempty"`
	IgnoreDirectoryErrorsSet bool `json:"ignoreDirErrsSet,omitempty"`
}

// Merge applies default values from the provided policy.
func (p *ErrorHandlingPolicy) Merge(src ErrorHandlingPolicy) {
	if !p.IgnoreFileErrorsSet && src.IgnoreFileErrorsSet {
		p.IgnoreFileErrors = src.IgnoreFileErrors
		p.IgnoreFileErrorsSet = true
	}

	if !p.IgnoreDirectoryErrorsSet && src.IgnoreDirectoryErrorsSet {
		p.IgnoreDirectoryErrors = src.IgnoreDirectoryErrors
		p.IgnoreDirectoryErrorsSet = true
	}
}

// defaultErrorHandlingPolicy is the default error handling policy.
var defaultErrorHandlingPolicy = ErrorHandlingPolicy{
	IgnoreFileErrors:         false,
	IgnoreFileErrorsSet:      true,
	IgnoreDirectoryErrors:    false,
	IgnoreDirectoryErrorsSet: true,
}
