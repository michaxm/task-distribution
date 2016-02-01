module Control.Distributed.Task.TaskSpawning.FunctionSerialization (serializeFunction, deserializeFunction) where

import Data.Binary (encode, decode)
import qualified Data.ByteString.Lazy as L
import Data.Typeable (Typeable)
import qualified GHC.Packing as P

import Control.Distributed.Task.Util.ErrorHandling

serializeFunction :: (Typeable a, Typeable b) => (a -> b) -> IO L.ByteString
serializeFunction = serialize

serialize :: Typeable a => a -> IO L.ByteString
serialize a = P.trySerialize a >>= return . encode

deserializeFunction :: (Typeable a, Typeable b) => L.ByteString -> IO (a -> b)
deserializeFunction = deserialize

deserialize :: (Typeable a) => L.ByteString -> IO a
deserialize bs = withErrorPrefix ("Could not deserialize: "++(show bs)) $ P.deserialize $ decode bs
