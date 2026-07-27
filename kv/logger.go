package kv

type Logger interface {
	Debug(msg string, fields map[string]any)
	Warn(msg string, fields map[string]any)
	Error(msg string, fields map[string]any)
}

type NoopLogger struct{}

func (NoopLogger) Debug(_ string, _ map[string]any) {}
func (NoopLogger) Warn(_ string, _ map[string]any)  {}
func (NoopLogger) Error(_ string, _ map[string]any) {}
