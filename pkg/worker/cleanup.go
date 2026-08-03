package worker

import (
	"log/slog"
	"os"
	"path/filepath"
)

// CleanupTempDir removes /tmp/{jobID} recursively after a job finishes
// processing, on both the success and failure paths (task 19). Deletion
// errors (e.g. permissions) are logged, not returned: cleanup is best-effort
// and must never fail job processing.
func CleanupTempDir(logger *slog.Logger, podID, jobID string) {
	dir := filepath.Join(os.TempDir(), jobID)
	if err := os.RemoveAll(dir); err != nil {
		LogJobError(logger, "temp_cleanup_failed", jobID, podID, err, 0, "", "")
		return
	}
	logger.Info("temp_cleanup_complete", "job_id", jobID, "pod_id", podID, "event_type", "temp_cleanup_complete", "dir", dir)
}
