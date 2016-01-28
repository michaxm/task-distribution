module DemoTask where

import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.Maybe

calculateRatio :: [BL.ByteString] -> [BL.ByteString]
calculateRatio = (:[]) . BLC.pack . show . divideRatio . foldr countHits (0, 0) . parseEntries
  where
    divideRatio :: (Int, Int) -> Double
    divideRatio (hits, count) = fromIntegral hits / (fromIntegral count)
    countHits :: Entry -> (Int, Int) -> (Int, Int)
    countHits e (hits, count) = (hits + if isHit e then 1 else 0, count+1)

isHit :: Entry -> Bool
isHit (_, _, vs) = maybe False notEmpty $ lookup (BLC.pack "searchCrit") vs
  where
    notEmpty :: BL.ByteString -> Bool
    notEmpty = (>0) . (read :: String -> Int) . BLC.unpack

parseEntries :: [BL.ByteString] -> [Entry]
parseEntries = catMaybes . map parseEntry

type Entry = (BL.ByteString, BL.ByteString, [(BL.ByteString, BL.ByteString)])

--nlrkwldbji|3101692061|xlerb:uubqpnce|searchCrit:0|longerAttr:nspixnozidyqweu|other:lju|otherRandomNam:tndhdno|otherRandomName2:dgkkj|otherRandomName3:yrdwgww|last:xvofbj
parseEntry :: BL.ByteString -> Maybe Entry
parseEntry str =
  let cells = BLC.split '|' str
  in if length cells < 2
     then Nothing
     else Just (cells !! 0, cells !! 1, catMaybes $ map parseValue $ drop 2 cells)
  where
    parseValue val = let kv = BLC.split ':' val in if length kv /= 2 then Nothing else Just (kv !! 0, kv !! 1)
