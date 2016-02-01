module Control.Distributed.Task.Util.ErrorHandling (withErrorPrefix, withErrorAction) where

import Control.Exception (catch, SomeException)

withErrorPrefix :: String -> IO a -> IO a
withErrorPrefix = withErrorHandling Nothing

withErrorAction :: (String -> IO ()) -> String -> IO a -> IO a
withErrorAction = withErrorHandling . Just

withErrorHandling :: Maybe (String -> IO ()) -> String -> IO a -> IO a
withErrorHandling errorAction prefix action = action `catch` wrapError
  where
    wrapError :: SomeException -> IO a
    wrapError e = let errorMessage = prefix++": "++(show e) in do
        maybe (return ()) (\eA -> eA errorMessage) errorAction
        error $ errorMessage
