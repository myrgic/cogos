package engine

// Arms the guard in hermesProfileCache.helperValues: under go test, a
// secrets.command helper outside a temp dir (the operator's real profile)
// is never executed.
func init() { hermesHelperUnderTest = true }
