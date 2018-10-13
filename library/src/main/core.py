import pandas as pd
import numpy as np

from pyspark import keyword_only
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import *
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

from scipy.sparse import csr_matrix
from sklearn.preprocessing import LabelEncoder


def pivot_table(df, val_col, idx_col, column_col):
    """
    :param df:
    :param val_col:
    :param idx_col:
    :param column_col:
    :return:
    """
    lbl_encoder_idx = LabelEncoder()
    lbl_encoder_column = LabelEncoder()

    idx_u = df[idx_col].sort_values().unique()
    col_u = df[column_col].sort_values().unique()

    # labels the sorted unique values
    lbl_encoder_idx.fit(idx_u)
    lbl_encoder_column.fit(col_u)

    idx_i = lbl_encoder_idx.transform(df[idx_col])
    idx_j = lbl_encoder_column.transform(df[column_col])

    nrows = len(lbl_encoder_idx.classes_)
    ncols = len(lbl_encoder_column.classes_)

    # creates the sparse pivot table
    A_piv = csr_matrix((df[val_col].values, (idx_i, idx_j)), shape=(nrows, ncols))

    return pd.SparseDataFrame(data=A_piv, index=list(idx_u), columns=list(col_u)).to_dense().fillna(0.0)


def create_cc_entries_without_spanish_lang(age_vec, gender_vec, his_vec, race_vec, cc_iter):
    """
    :param age_vec:
    :param gender_vec:
    :param his_vec:
    :param race_vec:
    :param cc_iter:
    :return:
    """
    cc_vec = np.zeros((age_vec.shape[0],))

    race_indices = (his_vec == 2) & (race_vec == 2)

    gender_indices = gender_vec == 1

    cc_vec[race_indices & gender_indices] = age_vec.loc[race_indices & gender_indices]
    cc_vec[race_indices & (~gender_indices)] = age_vec.loc[race_indices & (~gender_indices)] + cc_iter

    race_indices = his_vec == 1

    cc_vec[race_indices & gender_indices] = age_vec.loc[race_indices & gender_indices] + 2 * cc_iter
    cc_vec[race_indices & (~gender_indices)] = age_vec.loc[race_indices & (~gender_indices)] + 3 * cc_iter

    race_indices = (race_vec == 1) & (his_vec == 2)

    cc_vec[race_indices & gender_indices] = age_vec.loc[race_indices & gender_indices] + 4 * cc_iter
    cc_vec[race_indices & (~gender_indices)] = age_vec.loc[race_indices & (~gender_indices)] + 5 * cc_iter

    return pd.Series(cc_vec)


def create_cc_entries_with_spanish_lang(age_vec, gender_vec, his_vec, race_vec, span_vec, cc_iter):
    """
    :param age_vec:
    :param gender_vec:
    :param his_vec:
    :param race_vec:
    :param span_vec:
    :param cc_iter:
    :return:
    """
    cc_vec = np.zeros((age_vec.shape[0], ))

    race_indices = (his_vec == 2) & (race_vec == 2)

    gender_indices = gender_vec == 1

    cc_vec[race_indices & gender_indices] = age_vec.loc[race_indices & gender_indices]
    cc_vec[race_indices & (~gender_indices)] = age_vec.loc[race_indices & (~gender_indices)] + cc_iter

    race_indices = (his_vec == 1) & (span_vec.isin([1., 2., 3.]))

    cc_vec[race_indices & gender_indices] = age_vec.loc[race_indices & gender_indices] + 2 * cc_iter
    cc_vec[race_indices & (~gender_indices)] = age_vec.loc[race_indices & (~gender_indices)] + 3 * cc_iter

    race_indices = (his_vec == 1) & (~span_vec.isin([1., 2., 3.]) | np.isnan(span_vec))

    cc_vec[race_indices & gender_indices] = age_vec.loc[race_indices & gender_indices] + 4 * cc_iter
    cc_vec[race_indices & (~gender_indices)] = age_vec.loc[race_indices & (~gender_indices)] + 5 * cc_iter

    race_indices = (race_vec == 1) & (his_vec == 2)

    cc_vec[race_indices & gender_indices] = age_vec.loc[race_indices & gender_indices] + 6 * cc_iter
    cc_vec[race_indices & (~gender_indices)] = age_vec.loc[race_indices & (~gender_indices)] + 7 * cc_iter

    return pd.Series(cc_vec)


class CCTransformer(Transformer, HasInputCols, HasOutputCol):
    cc_iter = \
        Param(Params._dummy(), "cc_iter", 'cc_iter',
              "splits specified will be treated as errors.")

    handleInvalid = Param(Params._dummy(), "handleInvalid", "how to handle invalid entries. " +
                          "Options are 'skip' (filter out rows with invalid values), " +
                          "'error' (throw an error), or 'keep' (keep invalid values in a special " +
                          "additional bucket).",
                          typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, cc_iter=None, inputCols=None, outputCol=None):
        super(CCTransformer, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        self._setDefault(handleInvalid="error")

    @keyword_only
    def setParams(self, cc_iter=None, inputCols=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setCCIter(self, value):
        return self._set(cc_iter=value)

    def getCCIter(self):
        return self.getOrDefault(self.cc_iter)

    def _transform(self, dataset):
        out_col = self.getOutputCol()
        in_col = dataset[self.getInputCols()]

        cc_iter = self.getCCIter()

        if np.any([col.lower() == 'spanish_language1' for col in self.getInputCols()]):

            @pandas_udf(returnType=IntegerType())
            def f(age_vec, gender_vec, his_vec, race_vec, span_vec):
                return create_cc_entries_with_spanish_lang(age_vec, gender_vec, his_vec, race_vec, span_vec, cc_iter)

        else:
            @pandas_udf(returnType=IntegerType())
            def f(age_vec, gender_vec, his_vec, race_vec):
                return create_cc_entries_without_spanish_lang(age_vec, gender_vec, his_vec, race_vec, cc_iter)

        return dataset.withColumn(out_col, f(*in_col))