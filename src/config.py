from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_PASS: str
    BD_NAME: str

    @property
    def DATABASE_URL_asyncpg(self) -> str:
        """URI for asynchronous database connection"""
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.BD_NAME}"

    @property
    def DATABASE_URL_psycopg(self) -> str:
        return f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.BD_NAME}"

    model_config = SettingsConfigDict(env_file=".env", )
    # basedir = os.path.abspath(os.path.dirname(__file__))
    # print(basedir)


settings = Settings()
