# Copyright (C) 2026 Philipp Schüpbach
# This file is part of deng-ingestion-pipeline.
#
# deng-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# deng-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with deng-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from deng_ingestion.pipeline.job import PipelineJob
from deng_ingestion.steps.select_pending_silver_batch import (
    SelectPendingSilverBatchStep,
)
from deng_ingestion.steps.transform_all_pending_batches_to_silver import (
    TransformAllPendingBatchesToSilverStep,
)
from deng_ingestion.steps.transform_batch_to_silver import TransformBatchToSilverStep


def build_transform_events_job() -> PipelineJob:
    return PipelineJob(
        name="transform_events",
        steps=[
            SelectPendingSilverBatchStep(),
            TransformBatchToSilverStep(),
        ],
    )


def build_transform_all_events_job() -> PipelineJob:
    return PipelineJob(
        name="transform_all_events",
        steps=[
            TransformAllPendingBatchesToSilverStep(),
        ],
    )
