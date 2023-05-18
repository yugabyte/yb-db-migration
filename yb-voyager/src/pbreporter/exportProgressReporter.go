package pbreporter

import "github.com/vbauerster/mpb/v8"

type ExportProgressReporter interface { // Bare minimum required to simulate mpb.bar for exportDataStatus
	SetTotalRowCount(totalRowCount int64, triggerComplete bool)
	SetExportedRowCount(exportedRowCount int64)
	IsComplete() bool
}

func NewExportPB(progressContainer *mpb.Progress, tableName string, disablePb bool) ExportProgressReporter {
	if disablePb {
		return newDisablePBReporter()
	}
	return newEnablePBReporter(progressContainer, tableName)
}
