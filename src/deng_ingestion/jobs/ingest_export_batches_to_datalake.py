from __future__ import annotations

from dataclasses import dataclass, field

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    clear_archive_object_path,
    clear_archive_path,
    clear_csv_object_path,
    clear_current_batch,
    clear_extracted_csv_path,
    clear_remaining_registered_export_batch_ids,
    get_current_batch,
    get_processed_batches,
    get_registered_export_batch_ids,
    get_remaining_registered_export_batch_ids,
    get_uploaded_export_batch_ids,
    set_processed_batches,
    set_remaining_registered_export_batch_ids,
    set_uploaded_export_batch_ids,
)
from deng_ingestion.steps.datalake.build_object_paths import BuildExportObjectPathsStep
from deng_ingestion.steps.datalake.mark_batch_uploaded import MarkBatchUploadedStep
from deng_ingestion.steps.datalake.upload_export_archive_to_datalake import (
    UploadExportArchiveToDatalakeStep,
)
from deng_ingestion.steps.datalake.upload_export_csv_to_datalake import (
    UploadExportCsvToDatalakeStep,
)
from deng_ingestion.steps.export.download_export_archive import (
    DownloadExportArchiveStep,
)
from deng_ingestion.steps.export.extract_export_csv import ExtractExportCsvStep
from deng_ingestion.steps.export.select_registered_export_batch import (
    SelectRegisteredExportBatchStep,
)


@dataclass(frozen=True)
class IngestExportBatchesToDatalakeJob:
    name: str = "ingest_export_batches_to_datalake"

    select_batch_step: SelectRegisteredExportBatchStep = field(
        default_factory=SelectRegisteredExportBatchStep
    )
    download_archive_step: DownloadExportArchiveStep = field(
        default_factory=DownloadExportArchiveStep
    )
    extract_csv_step: ExtractExportCsvStep = field(default_factory=ExtractExportCsvStep)
    build_object_paths_step: BuildExportObjectPathsStep = field(
        default_factory=BuildExportObjectPathsStep
    )
    upload_archive_step: UploadExportArchiveToDatalakeStep = field(
        default_factory=UploadExportArchiveToDatalakeStep
    )
    upload_csv_step: UploadExportCsvToDatalakeStep = field(
        default_factory=UploadExportCsvToDatalakeStep
    )
    mark_uploaded_step: MarkBatchUploadedStep = field(
        default_factory=MarkBatchUploadedStep
    )

    def run(self, context: PipelineContext) -> None:
        remaining_batch_ids = get_remaining_registered_export_batch_ids(context)
        if remaining_batch_ids is None:
            set_remaining_registered_export_batch_ids(
                context,
                list(get_registered_export_batch_ids(context)),
            )

        uploaded_batch_ids = list(get_uploaded_export_batch_ids(context))
        processed_batches = get_processed_batches(context)

        while True:
            clear_current_batch(context)

            self.select_batch_step.run(context)

            current_batch = get_current_batch(context)
            if current_batch is None:
                break

            try:
                self.download_archive_step.run(context)
                self.extract_csv_step.run(context)

                self.build_object_paths_step.run(context)

                self.upload_archive_step.run(context)
                self.upload_csv_step.run(context)

                self.mark_uploaded_step.run(context)

                batch_id = current_batch.get("batch_id")
                if batch_id is None:
                    raise ValueError("Expected batch_id in current batch")

                uploaded_batch_ids.append(batch_id)
                set_uploaded_export_batch_ids(
                    context,
                    list(dict.fromkeys(uploaded_batch_ids)),
                )

                processed_batches += 1
                set_processed_batches(context, processed_batches)

            finally:
                clear_archive_path(context)
                clear_extracted_csv_path(context)
                clear_archive_object_path(context)
                clear_csv_object_path(context)
                clear_current_batch(context)

        clear_remaining_registered_export_batch_ids(context)


def build_ingest_registered_export_batches_to_datalake_job() -> (
    IngestExportBatchesToDatalakeJob
):
    return IngestExportBatchesToDatalakeJob()
