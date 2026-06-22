package kv

type Logger interface {
	Warn(msg string, fields map[string]any)
	Warnf(format string, args ...any)
}

type NoopLogger struct{}

func (NoopLogger) Warn(_ string, _ map[string]any) {}
func (NoopLogger) Warnf(_ string, _ ...any)        {}
