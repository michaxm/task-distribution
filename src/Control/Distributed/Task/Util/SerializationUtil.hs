module Control.Distributed.Task.Util.SerializationUtil where

import Data.Time.Calendar (Day(..))
import Data.Time.Clock (UTCTime(..), NominalDiffTime)

type TimeStamp = (Integer, Rational)

serializeTime :: UTCTime -> TimeStamp
serializeTime (UTCTime (ModifiedJulianDay d) f) = (d, toRational f)

deserializeTime :: TimeStamp -> UTCTime
deserializeTime (d, f) = UTCTime (ModifiedJulianDay d) (fromRational f)

serializeTimeDiff :: NominalDiffTime -> Rational
serializeTimeDiff = toRational

deserializeTimeDiff :: Rational -> NominalDiffTime
deserializeTimeDiff = fromRational

