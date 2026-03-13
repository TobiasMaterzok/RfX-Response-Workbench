from __future__ import annotations

from fastapi import APIRouter, Depends

from app.api.deps import get_container
from app.pipeline.config import (
    artifact_index_hashes,
    pipeline_config_json_schema,
    resolve_pipeline_selection,
)
from app.schemas.api import PipelineConfigResponse
from app.services.container import ServiceContainer

router = APIRouter(prefix="/api/pipeline-config", tags=["pipeline-config"])


@router.get("/default", response_model=PipelineConfigResponse)
def get_default_pipeline_config(
    container: ServiceContainer = Depends(get_container),
) -> PipelineConfigResponse:
    selection = resolve_pipeline_selection(container.settings)
    return PipelineConfigResponse(
        profile_name=selection.profile_name or "default",
        config_hash=selection.config_hash,
        index_config_hash=selection.index_config_hash,
        runtime_config_hash=selection.runtime_config_hash,
        artifact_index_hashes=artifact_index_hashes(selection).__dict__,
        config=selection.resolved_config,
        config_schema=pipeline_config_json_schema(),
    )
