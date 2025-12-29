{-# LANGUAGE OverloadedStrings #-}

module SSTableSpec (sstableTests) where

import Control.Monad.Trans.Reader (runReaderT)
import Internal.Fs (newMemFs)
import qualified Internal.MemTable as MemTable
import Internal.SSTable (lookupChain, write)
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

sstableTests :: TestTree
sstableTests =
  testGroup
    "SSTable"
    [ testCase "write/load/lookup" $ do
        fs <- newMemFs
        result <- runReaderT
          ( do
              _ <- write "data" 1 [(MemTable.Put, "k1", Just "v1")]
              sst <- write "data" 2 [(MemTable.Put, "k2", Just "v2")]
              lookupChain [sst] "k2"
          )
          fs
        result @?= Just "v2",
      testCase "newer SSTable tombstone hides older value" $ do
        fs <- newMemFs
        result <- runReaderT
          ( do
              older <- write "data" 1 [(MemTable.Put, "k1", Just "v1")]
              newer <- write "data" 2 [(MemTable.Del, "k1", Nothing)]
              lookupChain [newer, older] "k1"
          )
          fs
        result @?= Nothing,
      testCase "newer SSTable value overrides older value" $ do
        fs <- newMemFs
        result <- runReaderT
          ( do
              older <- write "data" 1 [(MemTable.Put, "k1", Just "old")]
              newer <- write "data" 2 [(MemTable.Put, "k1", Just "new")]
              lookupChain [newer, older] "k1"
          )
          fs
        result @?= Just "new",
      testCase "lookup missing key returns Nothing" $ do
        fs <- newMemFs
        result <- runReaderT
          ( do
              sst <- write "data" 1 [(MemTable.Put, "k1", Just "v1")]
              lookupChain [sst] "absent"
          )
          fs
        result @?= Nothing,
      testCase "multiple SSTables prefer newest match" $ do
        fs <- newMemFs
        result <- runReaderT
          ( do
              s1 <- write "data" 1 [(MemTable.Put, "k", Just "v1")]
              s2 <- write "data" 2 [(MemTable.Put, "k", Just "v2")]
              s3 <- write "data" 3 [(MemTable.Put, "k", Just "v3")]
              lookupChain [s3, s2, s1] "k"
          )
          fs
        result @?= Just "v3",
      testCase "tombstone in newest SSTable wins even if older has value" $ do
        fs <- newMemFs
        result <- runReaderT
          ( do
              s1 <- write "data" 1 [(MemTable.Put, "k", Just "v1")]
              s2 <- write "data" 2 [(MemTable.Del, "k", Nothing)]
              s3 <- write "data" 3 [(MemTable.Put, "k", Just "v3")]
              -- ordering matters: s2 newest here
              lookupChain [s2, s3, s1] "k"
          )
          fs
        result @?= Nothing,
      testCase "duplicate keys in a single SSTable pick the first occurrence" $ do
        -- current behavior: binary search hits the first matching entry
        fs <- newMemFs
        result <- runReaderT
          ( do
              sst <- write "data" 1 [(MemTable.Put, "k", Just "first"), (MemTable.Put, "k", Just "second")]
              lookupChain [sst] "k"
          )
          fs
        result @?= Just "first"
    ]
