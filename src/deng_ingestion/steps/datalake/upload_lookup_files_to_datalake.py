from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from deng_ingestion.pipeline.context import PipelineContext
from deng_ingestion.pipeline.context_access import (
    get_lookup_dir,
    get_lookup_object_paths,
    set_uploaded_lookup_object_paths,
)
from deng_ingestion.steps.datalake.object_storage import upload_file


@dataclass(frozen=True)
class UploadLookupFilesToDatalakeStep:
    name: str = "upload_lookup_files_to_datalake"

    def run(self, context: PipelineContext) -> None:
        lookup_dir = get_lookup_dir(context)
        if lookup_dir is None:
            raise ValueError("Expected lookup_dir in pipeline context")

        lookup_object_paths = get_lookup_object_paths(context)
        if not lookup_object_paths:
            raise ValueError("Expected lookup object paths in pipeline context")

        logger.info(
            "Uploading lookup files to datalake: file_count={}, lookup_dir={}",
            len(lookup_object_paths),
            lookup_dir,
        )

        uploaded_lookup_object_paths: dict[str, str] = {}

        for file_name, object_path in lookup_object_paths.items():
            local_file_path = lookup_dir / file_name

            if not local_file_path.exists():
                raise ValueError(f"Missing lookup file for upload: {local_file_path}")

            if not local_file_path.is_file():
                raise ValueError(f"Lookup upload path is not a file: {local_file_path}")

            upload_file(
                local_path=local_file_path,
                object_path=object_path,
            )

            uploaded_lookup_object_paths[file_name] = object_path

        set_uploaded_lookup_object_paths(context, uploaded_lookup_object_paths)

        logger.debug(
            "Finished lookup uploads to datalake: uploaded_file_count={}",
            len(uploaded_lookup_object_paths),
        )
