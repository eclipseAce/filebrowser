package hashfs

import "io"

func closeOnError(c io.Closer, err *error) error {
	if c != nil && *err != nil {
		return c.Close()
	}
	return nil
}
