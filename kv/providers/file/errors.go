package file

import "errors"

var (
	ErrBasePathRequired = errors.New("file: base_path required")
	ErrAbsoluteRejected = errors.New("file: absolute path rejected when base_path is set")
	ErrTraversal        = errors.New("file: path traversal detected")
	ErrSymlinkEscape    = errors.New("file: symlink escapes base_path")
	ErrEmptyKey         = errors.New("file: key must not be empty")
)
