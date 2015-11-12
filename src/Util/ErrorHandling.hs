module Util.ErrorHandling where

import Control.Exception (catch, SomeException)

addErrorPrefix :: String -> IO a -> IO a
addErrorPrefix prefix action = action `catch` wrapException
  where
    wrapException :: SomeException -> a
    wrapException e = error $ prefix++": "++(show e)
