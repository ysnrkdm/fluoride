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
import qualified Internal.MemTable as MemTable
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
        memLimit = 2 * 1024 * 1024
      }

closeDB :: DB -> IO ()
closeDB db = do
  db' <- flush db
  hClose (walH db')

putKV :: DB -> B.ByteString -> B.ByteString -> IO DB
putKV db k v = do
  WAL.append (walH db) 0 k (Just v)
  let mem' = MemTable.insert k v (mem db)
      db' = db {mem = mem'}
  if MemTable.isExceedingSizeBytes mem' (memLimit db') then flush db' else pure db'

delKV :: DB -> B.ByteString -> IO DB
delKV db k = do
  WAL.append (walH db) 1 k Nothing
  let mem' = MemTable.delete k (mem db)
  return db {mem = mem'}

getKV :: DB -> B.ByteString -> IO (Maybe B.ByteString)
getKV db k = do
  case MemTable.lookup k (mem db) of
    Just mv -> pure mv
    Nothing -> SSTable.lookupChain (ssts db) k

flush :: DB -> IO DB
flush db =
  if MemTable.isEmpty (mem db)
    then return db
    else do
      let pairs = MemTable.snapshotAsc (mem db)
      sst <- SSTable.write (rootDir db) (nextId db) pairs
      -- rotate WAL (truncating for now)
      hClose (walH db)
      let wal = rootDir db </> "wal.log"
      withFile wal WriteMode (\_ -> pure ())
      walH' <- openFile wal ReadWriteMode
      hSeek walH' SeekFromEnd 0
      return
        db
          { ssts = sst : ssts db,
            mem = MemTable.empty,
            nextId = nextId db + 1,
            walH = walH'
          }
