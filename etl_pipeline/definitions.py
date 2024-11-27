from dagster import Definitions, load_assets_from_modules

from etl_pipeline.assets import assets  # noqa: TID252
from etl_pipeline.resources import resources  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={"db_client": resources.db_client},
)
