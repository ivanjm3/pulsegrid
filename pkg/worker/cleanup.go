package worker

import (
	"log"
	"os"
	"path/filepath"
)

// CleanupTempDir removes /tmp/{jobID} recursively after a job finishes
// processing, on both the success and failure paths (task 19). Deletion
// errors (e.g. permissions) are logged, not returned: cleanup is best-effort
// and must never fail job processing.
func CleanupTempDir(jobID string) {
	dir := filepath.Join(os.TempDir(), jobID)
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("event=temp_cleanup_failed job_id=%s dir=%s error=%v", jobID, dir, err)
		return
	}
	log.Printf("event=temp_cleanup_complete job_id=%s dir=%s", jobID, dir)
}
