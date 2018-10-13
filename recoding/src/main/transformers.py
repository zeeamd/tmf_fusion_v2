import pandas as pd
import numpy as np

from pyspark.sql.functions import when
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType, DoubleType

from pyspark import keyword_only
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import *


class IfElseTransformer(Transformer, HasInputCol, HasOutputCol, HasHandleInvalid):

    vals = \
        Param(Params._dummy(), "vals",
              "Split points for mapping continuous features into buckets. With n+1 splits, " +
              "there are n buckets. A bucket defined by splits x,y holds values in the " +
              "range [x,y) except the last bucket, which also includes y. The splits " +
              "should be of length >= 3 and strictly increasing. Values at -inf, inf must be " +
              "explicitly provided to cover all Double values; otherwise, values outside the " +
              "splits specified will be treated as errors.")

    handleInvalid = Param(Params._dummy(), "handleInvalid", "how to handle invalid entries. " +
                          "Options are 'skip' (filter out rows with invalid values), " +
                          "'error' (throw an error), or 'keep' (keep invalid values in a special " +
                          "additional bucket).",
                          typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, vals=None, inputCol=None, outputCol=None):
        super(IfElseTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._setDefault(handleInvalid="error")

    @keyword_only
    def setParams(self, vals=None, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setVals(self, value):
        return self._set(vals=value)

    def getVals(self):
        return self.getOrDefault(self.vals)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCol()]

        vals = self.getVals()

        return dataset.withColumn(out_col, when(in_col.isin(vals), 1).otherwise(2))


class YesNoTransformer(IfElseTransformer):

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(YesNoTransformer, self).__init__(vals=['Y'], inputCol=inputCol, outputCol=outputCol)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)


class IsInTransformer(Transformer, HasInputCol, HasOutputCol):
    isin_bins = \
        Param(Params._dummy(), "isin_bins", "Maps values in list/regions to integer values")

    handleInvalid = Param(Params._dummy(), "handleInvalid", "how to handle invalid entries. " +
                          "Options are 'skip' (filter out rows with invalid values), " +
                          "'error' (throw an error), or 'keep' (keep invalid values in a special " +
                          "additional bucket).",
                          typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, isin_bins=None, inputCol=None, outputCol=None):
        super(IsInTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._setDefault(handleInvalid="error")

    @keyword_only
    def setParams(self, isin_bins=None, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setIsInBins(self, value):
        return self._set(isin_bins=value)

    def getIsInBins(self):
        return self.getOrDefault(self.isin_bins)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        isin_bins = self.getIsInBins()

        @pandas_udf(returnType=DoubleType())
        def f(series_col):

            n = len(isin_bins)

            res = np.empty((series_col.shape[0],))

            for i in range(n):
                res[series_col.isin(isin_bins[i])] = i + 1

            return pd.Series(res)

        return dataset.withColumn(out_col, f(in_col))


class MoreThanTransformer(Transformer, HasInputCols, HasOutputCol):

    handleInvalid = Param(Params._dummy(), "handleInvalid", "how to handle invalid entries. " +
                          "Options are 'skip' (filter out rows with invalid values), " +
                          "'error' (throw an error), or 'keep' (keep invalid values in a special " +
                          "additional bucket).",
                          typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        super(MoreThanTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._setDefault(handleInvalid="error")

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = self.getInputCols()

        if len(in_col) == 1:
            return dataset.withColumn(out_col, when(dataset[in_col[0]] > 0, 1).otherwise(2))

        else:
            return dataset.withColumn(out_col, when(dataset[in_col[0]] > dataset[in_col[1]], 1).otherwise(2))


class InlierMapperTransformer(Transformer, HasInputCols, HasOutputCol):
    values_mapper = \
        Param(Params._dummy(), "values_mapper", "Maps values in list/regions to integer values")

    handleInvalid = Param(Params._dummy(), "handleInvalid", "how to handle invalid entries. " +
                          "Options are 'skip' (filter out rows with invalid values), " +
                          "'error' (throw an error), or 'keep' (keep invalid values in a special " +
                          "additional bucket).",
                          typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, values_mapper=None, inputCols=None, outputCol=None):
        super(InlierMapperTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._setDefault(handleInvalid="error")

    @keyword_only
    def setParams(self, values_mapper=None, inputCols=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setValuesMapper(self, value):
        return self._set(values_mapper=value)

    def getValuesMapper(self):
        return self.getOrDefault(self.values_mapper)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = self.getInputCols()

        assert len(in_col) == 2

        values_mapped = self.getValuesMapper()

        inlier_val = values_mapped[0]

        inliner_col = in_col[0]
        outlier_col = in_col[1]

        new_val = values_mapped[1]

        return dataset.withColumn(out_col, when(dataset[inliner_col].isin(inlier_val),
                                                new_val).otherwise(dataset[outlier_col]))


class ClipperTransformer(Transformer, HasInputCol, HasOutputCol):
    clip_lower = \
        Param(Params._dummy(), "clip_lower", "Maps values in list/regions to integer values")

    clip_upper = \
        Param(Params._dummy(), "clip_upper", "Maps values in list/regions to integer values")

    handleInvalid = Param(Params._dummy(), "handleInvalid", "how to handle invalid entries. " +
                          "Options are 'skip' (filter out rows with invalid values), " +
                          "'error' (throw an error), or 'keep' (keep invalid values in a special " +
                          "additional bucket).",
                          typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, clip_lower=None, clip_upper=None, inputCol=None, outputCol=None):
        super(ClipperTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._setDefault(handleInvalid="error")

    @keyword_only
    def setParams(self, clip_lower=None, clip_upper=None, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setClipUpper(self, value):
        return self._set(clip_upper=value)

    def getClipUpper(self):
        return self.getOrDefault(self.clip_upper)

    def setClipLower(self, value):
        return self._set(clip_lower=value)

    def getClipLower(self):
        return self.getOrDefault(self.clip_lower)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        clip_upper = self.getClipUpper()
        clip_lower = self.getClipLower()

        return dataset.withColumn(out_col, when(dataset[in_col] < clip_lower, clip_lower).otherwise(
            when(clip_upper < dataset[in_col], clip_upper).otherwise(dataset[in_col])))
