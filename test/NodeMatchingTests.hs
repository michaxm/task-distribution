import Data.List.Split (splitOn)
import Test.Hspec

import ClusterComputing.DataLocality (nodeMatcher)

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "ClusterComputing.DataLocality" $ do
        context "Node/Host matching" $ do
            it "does not match everything" $ do
              nodeMatcher "nid://node:44441:0" "host" `shouldBe` False
            it "extracts host from nodeId string" $ do
              (dropWhile (=='/') . head . drop 1 . splitOn ":") "nid://localhost:44441:0" `shouldBe` "localhost"
            it "extracts host from host string" $ do
              (head . splitOn ":") "localhost:50010" `shouldBe` "localhost"
            it "matches same host" $ do
              nodeMatcher "nid://localhost:44441:0" "localhost:50010" `shouldBe` True
            it "matches special hdfs host" $ do
              nodeMatcher "nid://localhost:44441:0" "127.0.0.1:50010" `shouldBe` True
            it "matches special node host" $ do
              nodeMatcher "nid://127.0.0.1:44441:0" "localhost:50010" `shouldBe` True
