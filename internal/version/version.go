package version

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func String() string {
	return Version + " (commit: " + Commit + ", built: " + BuildTime + ")"
}
