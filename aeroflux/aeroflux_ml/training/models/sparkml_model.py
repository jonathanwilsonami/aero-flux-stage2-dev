"""Spark ML adapter — a REAL GBTClassifier that activates only when pyspark is
available and compute.backend='spark'. It converts the numpy matrix to a Spark
DataFrame, trains distributed, and predicts. Guarded so absence of Spark never
breaks the pipeline — this is the course's 'big data engine' extension point.

Not on the default path because Stage 2 data fits in memory (Polars); flip
compute.backend to 'spark' and add a `type: sparkml` model to use it."""
from __future__ import annotations

import logging

import numpy as np

from .base import BaseModel

log = logging.getLogger("aeroflux.training.models.spark")


def spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
        return True
    except Exception:
        return False


class SparkMLModel(BaseModel):
    kind = "sparkml"

    def _session(self):
        from pyspark.sql import SparkSession
        return (SparkSession.builder.appName("aeroflux-training")
                .master(self.params.get("master", "local[*]")).getOrCreate())

    def _to_spark(self, spark, X, y=None):
        import pandas as pd
        from pyspark.ml.feature import VectorAssembler
        cols = self.feature_names or [f"f{i}" for i in range(X.shape[1])]
        pdf = pd.DataFrame(np.nan_to_num(X), columns=cols)
        if y is not None:
            pdf["label"] = y
        sdf = spark.createDataFrame(pdf)
        sdf = VectorAssembler(inputCols=cols, outputCol="features",
                              handleInvalid="keep").transform(sdf)
        return sdf

    def fit(self, X, y, feature_names):
        if not spark_available():
            raise RuntimeError("pyspark not installed; `pip install pyspark` to use type: sparkml")
        from pyspark.ml.classification import GBTClassifier
        self.feature_names = feature_names
        spark = self._session()
        sdf = self._to_spark(spark, X, y)
        gbt = GBTClassifier(featuresCol="features", labelCol="label",
                            maxDepth=self.params.get("maxDepth", 6),
                            maxIter=self.params.get("maxIter", 100), seed=self.seed)
        self.model = gbt.fit(sdf)
        self._spark = spark
        log.info("fit Spark GBT '%s' on %d rows", self.name, len(y))
        return self

    def predict_proba(self, X):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        sdf = self._to_spark(self._spark, X)
        pred = self.model.transform(sdf).select("probability").collect()
        return np.array([float(r["probability"][1]) for r in pred])

    def save(self, path):
        self.model.save(path)          # native Spark model dir


class TensorFlowModel(BaseModel):
    """Extension-point STUB. Implement fit/predict with a Keras MLP when TF work
    begins; the interface is ready so the runner/registry need no changes."""
    kind = "tensorflow"

    def fit(self, X, y, feature_names):
        raise NotImplementedError(
            "TensorFlow model is a stub extension point. Implement a Keras MLP "
            "here (input=len(feature_names), sigmoid output) when ready.")

    def predict_proba(self, X):
        raise NotImplementedError

    def save(self, path):
        raise NotImplementedError
