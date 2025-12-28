{-# LANGUAGE OverloadedStrings #-}

import Control.Monad.IO.Class (liftIO)
import Internal.Fs (newMemFs)
import Lib (closeDB, delKV, flush, getKV, openDB, putKV, runApp)
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

main :: IO ()
main = defaultMain $ testGroup "fluoride" [crudWithMemFs]

crudWithMemFs :: TestTree
crudWithMemFs =
  testCase "basic CRUD with memFs" $ do
    fs <- newMemFs
    runApp fs $ do
      let key = "key1"
          val = "val1"
      -- initial read
      db0 <- openDB "testdb"
      v0 <- getKV db0 key
      liftIO $ v0 @?= Nothing
      -- write
      db1 <- putKV db0 key val
      v1 <- getKV db1 key
      liftIO $ v1 @?= Just val
      -- flush to SSTable
      db2 <- flush db1
      v2 <- getKV db2 key
      liftIO $ v2 @?= Just val
      closeDB db2
      -- reopen and read
      db3 <- openDB "testdb"
      v3 <- getKV db3 key
      liftIO $ v3 @?= Just val
      -- delete and flush
      db4 <- delKV db3 key
      v4 <- getKV db4 key
      liftIO $ v4 @?= Nothing
      db5 <- flush db4
      closeDB db5
      -- reopen after delete
      db6 <- openDB "testdb"
      v6 <- getKV db6 key
      liftIO $ v6 @?= Nothing
      closeDB db6
