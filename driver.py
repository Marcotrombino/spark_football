from abc import ABC, abstractmethod
from typing import Union
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame


class SparkDriver(ABC):
    app_name: str
    session: SparkSession

    @abstractmethod
    def reader_fn(self, *args, **kwargs) -> Union[RDD, DataFrame]:
        pass

    @abstractmethod
    def writer_fn(self, *args, **kwards) -> None:
        pass

    def __init__(self, app_name: str):
        self.app_name = app_name

    def start(self) -> None:
        self.session = SparkSession.builder.appName(self.app_name).getOrCreate()

    def stop(self) -> None:
        self.session.stop()

    def get_data(self, file_name: str, protocol: str, *args, **kwargs) -> Union[RDD, DataFrame]:
        return self.reader_fn(file_name, protocol, *args, **kwargs)

    def save(self, df: DataFrame, file_name: str, *args, **kwargs) -> None:
        return self.writer_fn(df, file_name, args, kwargs)
