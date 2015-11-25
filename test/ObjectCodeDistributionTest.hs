import Test.Hspec

import TaskSpawning.ObjectCodeModuleDeployment (buildLibs)

main :: IO ()
main = hspec $ do
  describe "TaskSpawning.ObjectCodeModuleDeployment" $ do
        context "build lib file paths" $ do
            it "maps empty" $ do
              buildLibs True "/home/user" "~/prefix" "ghc-7.8.4" []
                `shouldBe` ([], [])
            it "builds a correct lib path" $ do
              buildLibs True "/home/user" "~/prefix" "ghc-7.8.4" "bytestring-0.10.4.0"
                `shouldBe`
                (["/home/user/prefix/bytestring-0.10.4.0/libHSbytestring-0.10.4.0-ghc7.8.4.so"],
                 ["/home/user/prefix/bytestring-0.10.4.0/"])
            it "builds correct lib paths" $ do
              buildLibs True "/home/user" "~/prefix" "ghc-7.8.4" "bytestring-0.10.4.0\ndeepseq-1.3.0.2"
                `shouldBe`
                (["/home/user/prefix/bytestring-0.10.4.0/libHSbytestring-0.10.4.0-ghc7.8.4.so",
                  "/home/user/prefix/deepseq-1.3.0.2/libHSdeepseq-1.3.0.2-ghc7.8.4.so"
                 ],
                 ["/home/user/prefix/bytestring-0.10.4.0/",
                  "/home/user/prefix/deepseq-1.3.0.2/"])
