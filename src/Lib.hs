{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Lib
  ( openDB,
    closeDB,
    putKV,
    getKV,
    delKV,
    flush,
    DB (..),
    logInfo,
    runApp,
  )
where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Reader (ask, runReaderT)
import qualified Data.ByteString as B
import Data.List (sortOn)
import Internal.Fs (Fs (..), FsHandle)
import qualified Internal.MemTable as MemTable
import qualified Internal.SSTable as SSTable
import Internal.Types (AppM, DB (..))
import qualified Internal.WAL as WAL
import System.FilePath (takeExtension, (</>))
import System.IO (SeekMode (SeekFromEnd))

runApp :: Fs -> AppM a -> IO a
runApp fs action = runReaderT action fs

logInfo :: String -> AppM ()
logInfo = liftIO . putStrLn

withFs :: (Fs -> IO a) -> AppM a
withFs f = ask >>= liftIO . f

fsOpenRW' :: FilePath -> AppM FsHandle
fsOpenRW' path = withFs (\Fs {..} -> fsOpenRW path)

fsClose' :: FsHandle -> AppM ()
fsClose' h = withFs (\Fs {..} -> fsClose h)

fsSeek' :: FsHandle -> SeekMode -> Integer -> AppM ()
fsSeek' h mode off = withFs (\Fs {..} -> fsSeek h mode off)

fsListDir' :: FilePath -> AppM [FilePath]
fsListDir' p = withFs (\Fs {..} -> fsListDir p)

fsEnsureDir' :: FilePath -> AppM ()
fsEnsureDir' p = withFs (\Fs {..} -> fsEnsureDir p)

fsTruncate' :: FilePath -> AppM ()
fsTruncate' p = withFs (\Fs {..} -> fsTruncate p)

-- | Public APIs
openDB :: FilePath -> AppM DB
openDB dir = do
  logInfo $ "Opening DB at " ++ dir

  -- load SSTables
  logInfo "Loading SSTables..."
  fsEnsureDir' dir
  names <- fsListDir' dir
  let sstFiles = sortOn id [dir </> n | n <- names, "sst-" `elem` [take 4 n], ".dat" == takeExtension n]
  logInfo $ "Found " ++ show (length sstFiles) ++ " SSTables: " ++ show sstFiles
  ssts <- reverse <$> mapM SSTable.load sstFiles -- newest first

  -- WAL initialization
  logInfo "Opening WAL..."
  let wal = dir </> "wal.log"
  -- recover WAL
  mem <- WAL.recover wal
  -- WAL open/create
  walH <- fsOpenRW' wal
  fsSeek' walH SeekFromEnd 0

  return
    DB
      { rootDir = dir,
        walH,
        mem,
        ssts,
        nextId = length ssts + 1,
        memLimit = 2 * 1024 * 1024
      }

closeDB :: DB -> AppM ()
closeDB db = do
  db' <- flush db
  fsClose' (walH db')

putKV :: DB -> B.ByteString -> B.ByteString -> AppM DB
putKV db k v = do
  WAL.append (walH db) 0 k (Just v)
  let mem' = MemTable.insert k v (mem db)
      db' = db {mem = mem'}
  if MemTable.isExceedingSizeBytes mem' (memLimit db') then flush db' else pure db'

delKV :: DB -> B.ByteString -> AppM DB
delKV db k = do
  WAL.append (walH db) 1 k Nothing
  let mem' = MemTable.delete k (mem db)
  return db {mem = mem'}

getKV :: DB -> B.ByteString -> AppM (Maybe B.ByteString)
getKV db k = do
  case MemTable.lookup k (mem db) of
    Just mv -> pure mv
    Nothing -> SSTable.lookupChain (ssts db) k

flush :: DB -> AppM DB
flush db = do
  if MemTable.isEmpty (mem db)
    then return db
    else do
      let pairs = MemTable.snapshotAsc (mem db)
      sst <- SSTable.write (rootDir db) (nextId db) pairs
      -- rotate WAL (truncating for now)
      fsClose' (walH db)
      let wal = rootDir db </> "wal.log"
      fsTruncate' wal
      walH' <- fsOpenRW' wal
      fsSeek' walH' SeekFromEnd 0
      return
        db
          { ssts = sst : ssts db,
            mem = MemTable.empty,
            nextId = nextId db + 1,
            walH = walH'
          }
