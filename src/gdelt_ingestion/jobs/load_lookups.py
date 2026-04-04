# Copyright (C) 2026 Philipp Schüpbach
# This file is part of gdelt-ingestion-pipeline.
#
# gdelt-ingestion-pipeline is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# gdelt-ingestion-pipeline is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with gdelt-ingestion-pipeline. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

from gdelt_ingestion.pipeline.job import PipelineJob
from gdelt_ingestion.steps.download_lookup_files import DownloadLookupFilesStep
from gdelt_ingestion.steps.load_lookup_dimensions import LoadLookupDimensionsStep
from gdelt_ingestion.steps.seed_risk_category_mapping import SeedRiskCategoryMappingStep


def build_load_lookups_job() -> PipelineJob:
    return PipelineJob(
        name="load_lookups",
        steps=[
            DownloadLookupFilesStep(),
            LoadLookupDimensionsStep(),
            SeedRiskCategoryMappingStep(),
        ],
    )
