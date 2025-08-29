{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module Lib
  ( openDB,
    closeDB,
    putKV,
    getKV,
    delKV,
    flush,
    DB (..),
  )
where

import qualified Data.ByteString as B
import Data.List (sortOn)
import qualified Data.Map.Strict as M
import qualified Internal.SSTable as SSTable
import Internal.Types (DB (..))
import qualified Internal.WAL as WAL
import System.Directory (createDirectoryIfMissing, listDirectory)
import System.FilePath (takeExtension, (</>))
import System.IO (IOMode (..), SeekMode (..), hClose, hSeek, openFile, withFile)

-- | Public APIs
openDB :: FilePath -> IO DB
openDB dir = do
  putStrLn $ "Opening DB at " ++ dir

  -- load SSTables
  putStrLn "Loading SSTables..."
  createDirectoryIfMissing True dir
  names <- listDirectory dir
  let sstFiles = sortOn id [dir </> n | n <- names, "sst-" `elem` [take 4 n], ".dat" == takeExtension n]
  putStrLn $ "Found " ++ show (length sstFiles) ++ " SSTables: " ++ show sstFiles
  ssts <- reverse <$> mapM SSTable.load sstFiles -- newest first

  -- WAL initialization
  putStrLn "Opening WAL..."
  let wal = dir </> "wal.log"
  -- recover WAL
  mem <- WAL.recover wal
  -- WAL open/create
  walH <- openFile wal ReadWriteMode
  hSeek walH SeekFromEnd 0

  return
    DB
      { rootDir = dir,
        walH,
        mem,
        ssts,
        nextId = length ssts + 1,
        memLimit = 2 * 1024 * 1024,
        memBytes = 0
      }

closeDB :: DB -> IO ()
closeDB db = do
  db' <- flush db
  hClose (walH db')

putKV :: DB -> B.ByteString -> B.ByteString -> IO DB
putKV db k v = do
  WAL.append (walH db) 0 k (Just v)
  let mem' = M.insert k (Just v) (mem db)
      added = 9 + B.length k + B.length v
      db' = db {mem = mem', memBytes = memBytes db + added}
  if memBytes db' >= memLimit db' then flush db' else pure db'

delKV :: DB -> B.ByteString -> IO DB
delKV db k = do
  WAL.append (walH db) 1 k Nothing
  let mem' = M.insert k Nothing (mem db)
      added = 9 + B.length k
  return db {mem = mem', memBytes = memBytes db + added}

getKV :: DB -> B.ByteString -> IO (Maybe B.ByteString)
getKV db k = do
  case M.lookup k (mem db) of
    Just mv -> pure mv
    Nothing -> SSTable.lookupChain (ssts db) k

flush :: DB -> IO DB
flush db
  | M.null (mem db) = return db
  | otherwise = do
      let pairs = [(k, v) | (k, Just v) <- M.toAscList (mem db)]
      sst <- SSTable.write (rootDir db) (nextId db) pairs
      -- rotate WAL (truncating for now)
      hClose (walH db)
      let wal = rootDir db </> "wal.log"
      withFile wal WriteMode (\_ -> pure ())
      walH' <- openFile wal ReadWriteMode
      hSeek walH' SeekFromEnd 0
      return db {ssts = sst : ssts db, mem = M.empty, nextId = nextId db + 1, walH = walH', memBytes = 0}
