package help

import "errors"

func Error(hint []byte) error { return errors.New(string(hint)) }
