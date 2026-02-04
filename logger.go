package bitgask

type noopLogger struct{}

func (noopLogger) Printf(string, ...interface{}) {}

func logf(logger Logger, format string, args ...interface{}) {
	if logger == nil {
		return
	}
	logger.Printf(format, args...)
}
