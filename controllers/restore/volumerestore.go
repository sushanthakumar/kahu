/*
Copyright 2022 The SODA Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package restore

import (
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1"
)

func (ctx *restoreContext) syncVolumeRestore(restore *kahuapi.Restore) error {
	// all contents are prefetched based on restore backup spec
	ctx.logger.Infof("Restore in %s phase", kahuapi.RestoreStageVolumes)

	// check if restore has already scheduled for volume restore
	// if already scheduled for restore, continue to wait for completion

	return nil
}

func (ctx *restoreContext) VolumeRestore(restore *kahuapi.Restore) error {
	// all contents are prefetched based on restore backup spec
	ctx.logger.Infof("Restore in %s phase", kahuapi.RestoreStageVolumes)

	// check if restore has already scheduled for volume restore
	// if already scheduled for restore, continue to wait for completion

	return nil
}
