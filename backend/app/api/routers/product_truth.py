from __future__ import annotations

from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.api.deps import get_container, get_session, get_user_context
from app.models.enums import ReproducibilityMode
from app.pipeline.config import (
    resolve_pipeline_selection,
)
from app.services.container import ServiceContainer
from app.services.identity import UserContext
from app.services.product_truth import ProductTruthInput, ingest_product_truth_inputs
from app.services.reproducibility import product_truth_inline_manifest

router = APIRouter(prefix="/api/product-truth", tags=["product-truth"])


class ProductTruthRecordBody(BaseModel):
    product_area: str
    title: str
    body: str
    language: str
    source_file_name: str
    source_section: str
    effective_from: date
    effective_to: date | None = None
    version: str
    reproducibility_mode: Literal["best_effort", "strict_eval"] = "best_effort"


@router.post("/import")
def import_product_truth(
    body: ProductTruthRecordBody,
    container: ServiceContainer = Depends(get_container),
    session: Session = Depends(get_session),
    user_context: UserContext = Depends(get_user_context),
) -> dict[str, str]:
    pipeline = resolve_pipeline_selection(container.settings)
    record_payload = body.model_dump(exclude={"reproducibility_mode"})
    record = ingest_product_truth_inputs(
        session,
        storage=container.storage,
        ai_service=container.ai_service,
        tenant_id=user_context.tenant.id,
        records=[ProductTruthInput(**record_payload)],
        source_manifest_json=product_truth_inline_manifest(
            records=[record_payload],
            pipeline_config_hash=pipeline.config_hash,
            index_config_hash=pipeline.index_config_hash,
        ),
        run_inputs_json={"source": "api_inline", "record_count": 1},
        settings=container.settings,
        reproducibility_mode=ReproducibilityMode(body.reproducibility_mode),
    )
    return {"truth_record_id": str(record[0].id)}
