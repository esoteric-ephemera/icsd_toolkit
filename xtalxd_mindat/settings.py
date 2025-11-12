"""Define basic settings for the ICSD API client."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MindatClientSettings(BaseSettings):

    API_KEY: str | None = Field(None, description="Mindat API key.")

    MAX_RETRIES: int | None = Field(
        10, description="The maximum number of retries when querying the ICSD API."
    )

    API_ENDPOINT: str | None = Field(
        "https://api.mindat.org", description="The API endpoint to use."
    )

    TIMEOUT: float | None = Field(
        15.0, description="The time in seconds to wait for a query to complete."
    )

    MAX_BATCH_SIZE: int | None = Field(
        500,
        description=(
            "The maximum number of structures to retrieve "
            "during pagination of query results."
        ),
    )

    model_config = SettingsConfigDict(env_prefix="MINDAT_API_")
