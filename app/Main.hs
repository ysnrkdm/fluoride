{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Lib

main :: IO ()
main = do
  db <- openDB "testdb"

  putStrLn "== read key1 for the restart =="
  v0 <- getKV db "key1"
  putStrLn $ "===> " ++ show v0 -- Expected: Just "val1" (from sst)
  --
  putStrLn ""
  putStrLn "== put key1 -> val1 =="
  db1 <- putKV db "key1" "val1"
  v0 <- getKV db1 "key1"
  putStrLn $ "===> " ++ show v0 -- Expected: Just "val1" (from memtable)
  --
  putStrLn ""
  putStrLn "== flush =="
  db2 <- flush db1
  v1 <- getKV db2 "key1"
  putStrLn $ "===> " ++ show v1 -- Expected: Just "val1" (from SST)
  --
  putStrLn ""
  putStrLn "== close and reopen =="
  closeDB db2
  db3 <- openDB "testdb"
  v2 <- getKV db3 "key1"
  putStrLn $ "===> " ++ show v2 -- Expected: Just "val1" (can read after recovery)
  --
  putStrLn ""
  putStrLn "== delete key1 =="
  db4 <- delKV db3 "key1"
  v3 <- getKV db4 "key1"
  putStrLn $ "===> " ++ show v3 -- Expected: Nothing (tombstone in memtable should cover the deletion)
  --
  putStrLn ""
  putStrLn "== flush and reopen after delete =="
  db5 <- flush db4
  closeDB db5
  db6 <- openDB "testdb"
  v4 <- getKV db6 "key1"
  putStrLn $ "===> " ++ show v4 -- Expected: Nothing (key1 should be gone from memtable and SST)
