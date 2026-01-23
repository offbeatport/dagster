# burningdemand_dagster/utils/llm_schema.py
import json
import re
from typing import Literal

from pydantic import BaseModel, Field


class IssueLabel(BaseModel):
    canonical_title: str = Field(max_length=80)
    category: Literal[
        "ai",
        "finance",
        "compliance",
        "logistics",
        "healthtech",
        "devtools",
        "ecommerce",
        "other",
    ]
    description: str
    would_pay_signal: bool
    impact_level: Literal["low", "medium", "high"]


def extract_first_json_obj(text: str) -> dict:
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        raise ValueError("No JSON object found in model output")
    return json.loads(m.group(0))
