# -*- coding: utf-8 -*-
"""
Created on Fri Jun 29 13:08:30 2018

@author: tued7001
"""
import numpy as np
from scipy.sparse import hstack

from scipy.special import huber

from .cvxpy_solver import solver as cv_solver


class SoftCalibrator(object):
    
    def __init__(self, epsilon, solver = 'cvxpy', fit_intercept=False, budjet=2.0):
        '''
        Arguments:
            epsilon - huber parameter
            solver - the type of solver to use
            fit_intercept - whether we want to predict our bias
            budjet - how much diverence our new weights can have
        '''
        self.eps = epsilon
        self.solver = solver
        self.fit_intercept = fit_intercept
        self.budj = budjet
        self._adj_w = 1

    @property
    def coefficient(self):
        return self._adj_w

    @coefficient.setter
    def coefficient(self, val):

        assert isinstance(val, np.core.ndarray)

        self._adj_w = val

    @property
    def intercept(self):
        return self._intercept

    @intercept.setter
    def intercept(self, val):
        assert isinstance(val, float) | isinstance(val, int)

        self._intercept = val

    def fit(self, X, y, w_bounds, y_bounds, w_radi, sample_weights = 'None'):
        """
        :param X:
        :param y:
        :param w_bounds:
        :param y_bounds:
        :param w_radi:
        :param sample_weights:
        :return:
        """
        
        if isinstance(sample_weights, str):
            y_sample_weights = np.ones(y.shape[0])
            y_sample_weights *= (1.0 / y_sample_weights.shape[0] )
        else:
            y_sample_weights = sample_weights
        
        if self.fit_intercept:
            X_train = hstack([X, np.ones(X.shape[0])])
        else:
            X_train = X
        
        if self.solver == 'cvxpy':
            w_sol, solve_status = cv_solver(X_train, y, self.eps, w_bounds, y_bounds, w_radi, y_sample_weights,
                                            self.budj)
                        
        else:
            print('Solver not implemented')
            return self
        
        self.solve_status = solve_status
        
        if self.fit_intercept:
            self.coefficient = w_sol[:-1]
            self.intercept = w_sol[-1]
        else:
            self.coefficient = w_sol
            self.intercept = 0.0
        
        return self

    def predict(self, X):
        '''
        Arguments:
            X - Test dataset
        '''
        return X.dot(self.coefficient) + self.intercept
    
    def score(self, X_val, y_val, sample_weights='None'):
        '''
        This returns the huber loss of our validation
        '''
        if isinstance(sample_weights, str):
            sample_weights = np.ones(y_val.shape[0])
            sample_weights *= (1.0 / sample_weights.shape[0])
        
        res = self.predict(X_val) - y_val
        return np.dot(sample_weights, huber(self.eps, res) * 2.0 * self.eps)

    def weight_loss(self, radii):
        return np.dot(radii, np.square(self.coefficient - 1))
