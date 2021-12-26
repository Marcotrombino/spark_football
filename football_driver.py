from typing import Union, Optional
from driver import SparkDriver
from pyspark import RDD
from pyspark.sql import DataFrame
from pyspark.sql import functions as fn
from pyspark.sql.types import StructType, StringType, IntegerType, DateType, FloatType


class FootballSparkDriver(SparkDriver):
    def __init__(self, app_name: str):
        super(FootballSparkDriver, self).__init__(app_name)

    def reader_fn(self, file_name: str, protocol: str) -> Optional[Union[RDD, DataFrame]]:
        df_obj = self.session.read \
            .option("multiline", "true") \
            .option("quote", '"') \
            .option("header", "true") \
            .option("escape", "\\") \
            .option("escape", '"')

        if protocol == "file":
            return df_obj.csv("file:///" + file_name)
        else:
            return None

    def writer_fn(self, df: DataFrame, file_name: str, *args, **kwargs) -> None:
        df.coalesce(1)\
            .write.format("csv")\
            .option("header", "true")\
            .save(file_name)

    def clean_column(self, df: DataFrame, col_name: str) -> DataFrame:
        if hasattr(self, col_name + "_cleaner"):
            return getattr(self, col_name + "_cleaner")(df, col_name)
        else:
            print("Can't find \"{name}_cleaner\" method".format(name=col_name))
            return df

    def get_column_by_name(self, df: DataFrame, name: str) -> DataFrame:
        return df.select(name)

    # ---------------- default cleaners ----------------

    def trim_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return df.withColumn(col_name, fn.trim(fn.col(col_name)))

    def url_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
            col_name,
            fn.when(
                fn.col(col_name).rlike(
                    r'(?i)^https?://'  # http:// or https://
                    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
                    r'localhost|'  # localhost...
                    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
                    r'(?::\d+)?'  # optional port
                    r'(?:/?|[/?]\S+)$'
                ),
                fn.col(col_name)
            ).otherwise(None)
        )

    def performance_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
            col_name,
            fn.when(
                fn.col(col_name).rlike("^[0-9]{1,3}$"),
                fn.when(
                    fn.col(col_name).cast(IntegerType()).between(0, 100),
                    fn.col(col_name)
                ).otherwise(None)
            ).otherwise(None)
        )

    def total_performance_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
            col_name,
            fn.when(
                fn.col(col_name).rlike("^[0-9]+"),
                fn.col(col_name)
            ).otherwise(None)
        )

    def date_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name)\
            .withColumn(
                col_name,
                fn.to_date(fn.col(col_name), "MMM d, yyyy")
            )

    def price_cleaner(self, df: DataFrame, col_name: str, to_unit: str = "K") -> DataFrame:
        if to_unit == "K":     # to K
            return self.trim_cleaner(df, col_name)\
                .withColumn(
                    col_name,
                    fn.when(
                        fn.col(col_name).rlike("^€[0-9\.]+(K|M)?$"),
                        fn.when(    # convert no unit to K
                            ~fn.col(col_name).contains("M") & ~fn.col(col_name).contains("K"),
                            fn.regexp_replace(col_name, "€", "").cast(FloatType()) / 1000
                        ).when(     # convert M to K
                            fn.col(col_name).contains("M"),
                            fn.regexp_replace(col_name, "(€|M)", "").cast(FloatType()) * 1000
                        ).otherwise(    # already K
                            fn.regexp_replace(col_name, "(€|K)", "")
                        )
                    ).otherwise(None)
                ) \
                .withColumn(col_name, fn.round(fn.col(col_name).cast(FloatType()), 2))
        else:   # to M
            return self.trim_cleaner(df, col_name) \
                .withColumn(
                    col_name,
                    fn.when(
                        fn.col(col_name).rlike("^€[0-9\.]+(K|M)?$"),
                        fn.when(    # convert no unit to M
                            ~fn.col(col_name).contains("M") & ~fn.col(col_name).contains("K"),
                            fn.regexp_replace(col_name, "€", "").cast(FloatType()) / 1000000
                        ).when(     # convert K to M
                            fn.col(col_name).contains("K"),
                            fn.regexp_replace(col_name, "(€|K)", "").cast(FloatType()) / 1000
                        ).otherwise(    # already M
                            fn.regexp_replace(col_name, "(€|M)", "")
                        )
                    ).otherwise(None)
                ) \
                .withColumn(col_name, fn.round(fn.col(col_name).cast(FloatType()), 2))

    def stars_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
                col_name,
                fn.when(
                    fn.col(col_name).rlike("^[0-9]$"),
                    fn.when(
                        fn.col(col_name).cast(IntegerType()).between(0, 5),
                        fn.col(col_name)
                    ).otherwise(None)
                ).otherwise(None)
            )

    def work_rate_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name)\
            .withColumn(
                col_name,
                fn.when(
                    fn.lower(fn.col(col_name)).isin("low", "medium", "high"),
                    fn.col(col_name)
                ).otherwise(None)
            ) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    # ---------------- custom cleaners ----------------

    def index_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def profile_url_cleaner(self, df: DataFrame, col_name: str):
        return self.url_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def update_version_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
            col_name,
            fn.when(
                fn.col(col_name).rlike("^[0-9]{6}$"),
                fn.col(col_name)
            ).otherwise(None)
        ) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def picture_url_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.url_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def full_name_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def short_name_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def age_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
            col_name,
            fn.when(
                fn.col(col_name).rlike("^[0-9]{2}$"),
                fn.when(
                    fn.col(col_name).cast(IntegerType()).between(16, 65),
                    fn.col(col_name)
                ).otherwise(None)
            ).otherwise(None)
        ) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def overall_rating_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def potential_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def team_name_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def team_profile_url_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.url_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def team_picture_url_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.url_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def team_contract_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(  # remove useless characters from team_contract column
                col_name,
                fn.regexp_replace(fn.col(col_name), "[\n\r\"\\s]", "")
            ) \
            .withColumn(  # create team_contract_start column
                col_name + "_start",
                fn.when(  # get left-side date only if the split by ~ has 2 elements
                    fn.size(fn.split(fn.col(col_name), "~")) == 2,
                    fn.split(fn.col(col_name), "~").getItem(0)
                ).otherwise(None)
            ) \
            .withColumn(  # create team_contract_end column
                col_name + "_end",
                fn.when(  # get right-side date only if the split by ~ has 2 elements
                    fn.size(fn.split(fn.col(col_name), "~")) == 2,
                    fn.split(fn.col(col_name), "~").getItem(1)
                ).otherwise(None)
            ) \
            .withColumn(
                col_name,
                fn.col(col_name).cast(StringType())
            ) \
            .withColumn(  # convert string to date
                col_name + "_start",
                fn.to_date(fn.col(col_name + "_start"), "y")
            ) \
            .withColumn(  # convert string to date
                col_name + "_end",
                fn.to_date(fn.col(col_name + "_end"), "y")
            )

    def id_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
                col_name,
                fn.when(
                    fn.col(col_name).rlike("^[0-9]{1,8}$"),
                    fn.col(col_name)
                ).otherwise(None)
            ) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def height_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
                col_name,
                fn.when(
                    fn.col(col_name).rlike("^[0-9\.]+cm$"),
                    fn.when(
                        fn.regexp_replace(col_name, "cm", "").cast(FloatType()).between(120, 240),
                        fn.round(fn.regexp_replace(col_name, "cm", ""), 2)
                    ).otherwise(None)
                ).otherwise(None)
            )\
            .withColumn(col_name, fn.col(col_name).cast(FloatType()))

    def weight_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name) \
            .withColumn(
                col_name,
                fn.when(
                    fn.col(col_name).rlike("^[0-9\.]+kg$"),
                    fn.when(
                        fn.regexp_replace(col_name, "kg", "").cast(FloatType()).between(40, 115),
                        fn.round(fn.regexp_replace(col_name, "kg", ""), 2)
                    ).otherwise(None)
                ).otherwise(None)
            ) \
            .withColumn(col_name, fn.col(col_name).cast(FloatType()))

    def preferred_foot_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name)\
            .withColumn(
                col_name,
                fn.when(
                    fn.lower(fn.col(col_name)).isin("left", "right"),
                    fn.col(col_name)
                ).otherwise(None)
            ) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def best_overall_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def best_position_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.trim_cleaner(df, col_name)\
            .withColumn(
                col_name,
                fn.when(
                    fn.lower(fn.col(col_name)).isin(
                        "ls", "st", "rs",
                        "lw", "lf", "cf", "rf", "rw",
                        "lam", "cam", "ram",
                        "lm", "lcm", "cm", "rcm", "rm",
                        "lwb", "ldm", "cdm", "rdm", "rwb",
                        "lb", "lcb", "cb", "rcb", "rb",
                        "gk"
                    ),
                    fn.col(col_name)
                ).otherwise(None)
            ) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def growth_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def joined_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.date_cleaner(df, col_name)

    def loan_date_end_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.date_cleaner(df, col_name)

    def value_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.price_cleaner(df, col_name, to_unit="M")\
            .withColumn(col_name, fn.col(col_name).cast(FloatType()))

    def wage_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.price_cleaner(df, col_name, to_unit="K") \
            .withColumn(col_name, fn.col(col_name).cast(FloatType()))

    def release_clause_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.price_cleaner(df, col_name, to_unit="M") \
            .withColumn(col_name, fn.col(col_name).cast(FloatType()))

    def total_attacking_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def crossing_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def finishing_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def heading_accuracy_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def short_passing_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def volleys_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def total_skill_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def dribbling_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def curve_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def fk_accuracy_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def long_passing_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def ball_control_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def total_movement_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def acceleration_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def sprint_speed_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def agility_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def reactions_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def balance_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def total_power_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def shot_power_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def jumping_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def stamina_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def strength_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def long_shots_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def total_mentality_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def aggression_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def interceptions_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def positioning_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def vision_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def penalties_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def composure_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def total_defending_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def marking_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def standing_tackle_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def sliding_tackle_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def total_goalkeeping_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def gk_diving_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def gk_handling_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def gk_kicking_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def gk_positioning_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def gk_reflexes_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def total_stats_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def base_stats_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.total_performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def weak_foot_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.stars_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def skill_moves_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.stars_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def attacking_work_rate_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.work_rate_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def defensive_work_rate_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.work_rate_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(StringType()))

    def international_reputation_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.stars_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def pac_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def sho_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def pas_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def dri_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def def__cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))

    def phy_cleaner(self, df: DataFrame, col_name: str) -> DataFrame:
        return self.performance_cleaner(df, col_name) \
            .withColumn(col_name, fn.col(col_name).cast(IntegerType()))
