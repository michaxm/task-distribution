module VisitCalculation (calculateVisits) where

import Data.Foldable (foldl')
import Data.List.Split (splitOn)
import qualified Data.Map as Map
import Data.Map (Map)
import Text.JSON

type ParsedEvent = (VisitId, RequestTime, Event)
type VisitData = (VisitId, (RequestTime, [Event])) -- events sorted in reverse order
type VisitId = String
type RequestTime = String
type Event = String

calculateVisits :: [String] -> [String]
calculateVisits = map formatVisit . partitionByVisitorId . parseEvents

parseEvents :: [String] -> [ParsedEvent]
parseEvents = foldl' (\p -> maybe p (:p) . parseEvent) []
  where
    parseEvent :: String -> Maybe ParsedEvent
    parseEvent e =
      let first5 = take 5 (splitOn "|" e) in
       if length first5 < 5
       then Nothing
       else Just (first5 !! 4, first5 !! 0, e)

partitionByVisitorId :: [ParsedEvent] -> [VisitData]
partitionByVisitorId = Map.assocs . foldr insertEvent Map.empty
  where
    insertEvent :: ParsedEvent -> Map VisitId (RequestTime, [Event]) -> Map VisitId (RequestTime, [Event])
    insertEvent (k, t, e) = Map.insertWith mergeData k (t, [e])
      where
        mergeData :: (RequestTime, [Event]) -> (RequestTime, [Event]) -> (RequestTime, [Event])
        mergeData (newT, newEs) (oldT, oldEs) = (min newT oldT, reverse newEs ++ oldEs)

formatVisit :: VisitData -> String
formatVisit (i, (t, es)) = encode $ toJSObject [visitId, events]
  where
    events, visitId :: (String, JSValue)
    events = ("events", JSArray $ map (JSString . toJSString) es)
    visitId = ("visitId", JSObject $ toJSObject [visitorId, lastRequestTime])
      where
        visitorId, lastRequestTime :: (String, JSValue)
        visitorId = ("visitorId", JSObject $ toJSObject [("value", JSString $ toJSString i)])
        lastRequestTime = ("lastRequestTime", JSString $ toJSString t)
