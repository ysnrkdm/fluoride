{-# LANGUAGE OverloadedStrings #-}

module MemTableSpec (memTableTests) where

import Prelude hiding (lookup)
import Internal.MemTable
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

memTableTests :: TestTree
memTableTests =
  testGroup
    "MemTable"
    [ testCase "insert and lookup" $ do
        let mt = insert "k1" "v1" empty
        lookup "k1" mt @?= Just (Just "v1"),
      testCase "delete marks tombstone" $ do
        let mt = delete "k1" empty
        lookup "k1" mt @?= Just Nothing,
      testCase "snapshotAsc preserves ordering" $ do
        let mt = insert "b" "2" $ insert "a" "1" empty
        snapshotAsc mt @?= [(Put, "a", Just "1"), (Put, "b", Just "2")],
      testCase "bytes and limit tracking" $ do
        let mt = insert "k1" "v1" empty
            size = sizeBytes mt
        size > 0 @?= True
        isExceedingSizeBytes mt (size - 1) @?= True
        isExceedingSizeBytes mt size @?= False,
      testCase "delete increases bytes (monotonic usage accounting)" $ do
        let mt1 = insert "k1" "v1" empty
            mt2 = delete "k1" mt1
        sizeBytes mt2 >= sizeBytes mt1 @?= True
    ]
