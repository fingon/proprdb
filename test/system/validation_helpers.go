package genexample

import (
	"errors"
	"strings"
)

func (p *Person) Valid() error {
	if strings.TrimSpace(p.GetName()) == "" {
		return errors.New("name is required")
	}
	return nil
}
