from sklearn.base import BaseEstimator, TransformerMixin


class SignalLabelTransformer(BaseEstimator, TransformerMixin):
    """
    Signal has -1, 0, 1 values
    Sparse targets should be 0,1,2
    """

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        """ -1,0,1 to 1,2,3"""
        return X.values+1

    def inverse_transform(self, X, y=None):
        """ 1,2,3 to -1,0,1"""
        return X-1