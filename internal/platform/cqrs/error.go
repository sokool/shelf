package cqrs

import (
	"fmt"
)

// todo this section belongs to DDD not CQRS
type DomainError string

func (e DomainError) Args(args ...interface{}) DomainError {
	return DomainError(fmt.Sprintf(string(e), args...))
}

func (e DomainError) Error() string {
	return string(e)
}

func Errors(vv ...Validator) error {
	var str string
	for i := range vv {
		if err := vv[i].Validate(); err != nil {
			str += fmt.Sprintf("%s\n", err.Error())
		}
	}

	if str != "" {
		return fmt.Errorf(str)
	}

	return nil
}
