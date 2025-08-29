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

import Control.Monad (forM)
import Data.Binary.Get (getWord32be, getWord64be, runGet)
import Data.Binary.Put (putByteString, putWord32be, putWord64be, putWord8, runPut)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int64)
import Data.List (sortOn)
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import Data.Word (Word8)
import Debug.Trace (trace)
import System.Directory (createDirectoryIfMissing, doesFileExist, listDirectory)
import System.FilePath (takeExtension, (</>))
import System.IO (Handle, IOMode (..), SeekMode (..), hClose, hFileSize, hFlush, hSeek, hTell, openFile, withBinaryFile, withFile)

data SSTable = SSTable
  { sstablePath :: FilePath,
    sstableIndex :: V.Vector (B.ByteString, Int64) -- (key, offset-of-entry)
  }
  deriving (Show)

data DB = DB
  { rootDir :: (FilePath),
    walH :: Handle,
    mem :: !(M.Map B.ByteString (Maybe B.ByteString)), -- Nothing=tombstone
    ssts :: ![SSTable], -- newest first
    nextId :: !Int,
    memLimit :: !Int, -- bytes rough limit
    memBytes :: !Int
  }

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
  ssts <- reverse <$> mapM loadSSTable sstFiles -- newest first

  -- WAL initialization
  putStrLn "Opening WAL..."
  let wal = dir </> "wal.log"
  -- recover WAL
  mem <- recoverFromWAL wal
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
  appendWAL (walH db) 0 k (Just v)
  let mem' = M.insert k (Just v) (mem db)
      added = 9 + B.length k + B.length v
      db' = db {mem = mem', memBytes = memBytes db + added}
  if memBytes db' >= memLimit db' then flush db' else pure db'

delKV :: DB -> B.ByteString -> IO DB
delKV db k = do
  appendWAL (walH db) 1 k Nothing
  let mem' = M.insert k Nothing (mem db)
      added = 9 + B.length k
  return db {mem = mem', memBytes = memBytes db + added}

getKV :: DB -> B.ByteString -> IO (Maybe B.ByteString)
getKV db k = do
  case M.lookup k (mem db) of
    Just mv -> pure mv
    Nothing -> lookupSSTables (ssts db) k

flush :: DB -> IO DB
flush db
  | M.null (mem db) = return db
  | otherwise = do
      let pairs = [(k, v) | (k, Just v) <- M.toAscList (mem db)]
      sst <- writeSSTable (rootDir db) (nextId db) pairs
      -- rotate WAL (truncating for now)
      hClose (walH db)
      let wal = rootDir db </> "wal.log"
      withFile wal WriteMode (\_ -> pure ())
      walH' <- openFile wal ReadWriteMode
      hSeek walH' SeekFromEnd 0
      return db {ssts = sst : ssts db, mem = M.empty, nextId = nextId db + 1, walH = walH', memBytes = 0}

-- | WAL
appendWAL :: Handle -> Word8 -> B.ByteString -> Maybe B.ByteString -> IO ()
appendWAL h op k mv = do
  let bs = runPut $ do
        putWord8 op
        putWord32be (fromIntegral $ B.length k)
        putWord32be (fromIntegral $ maybe 0 B.length mv)
        putByteString k
        maybe (pure ()) putByteString mv
  BL.hPut h bs >> hFlush h

recoverFromWAL :: FilePath -> IO (M.Map B.ByteString (Maybe B.ByteString))
recoverFromWAL wal = do
  exists <- doesFileExist wal
  if not exists
    then return M.empty
    else do
      contents <- B.readFile wal
      return (go M.empty contents)
  where
    go m s
      | B.null s = m
      | otherwise =
          let (op, s1) = B.splitAt 1 s
              (klenBs, s2) = get32 s1
              (vlenBs, s3) = get32 s2
              (k, s4) = B.splitAt (fromIntegral klenBs) s3
              (v, rest) = B.splitAt (fromIntegral vlenBs) s4
              m' = case B.head op of
                0 -> M.insert k (Just v) m
                1 -> M.insert k Nothing m
                _ -> error "corrupted WAL"
           in go m' rest

    get32 t =
      let (bs, rest) = B.splitAt 4 t
          x = runGet getWord32be (BL.fromStrict bs)
       in (x, rest)

-- | SStables
writeSSTable :: FilePath -> Int -> [(B.ByteString, B.ByteString)] -> IO SSTable
writeSSTable dir sid kvsAsc = do
  let path = dir </> sstName sid
  withBinaryFile path WriteMode $ \h -> do
    -- write entries
    _offsets <- forM kvsAsc $ \(k, v) -> do
      off <- hTell h
      B.hPut h $ BL.toStrict $ encodeEntry k v
      return (k, off)
    B.hPut h $ BL.toStrict $ runPut $ putWord64be (fromIntegral $ length kvsAsc)
    hFlush h
    return ()
  loadSSTable path

loadSSTable :: FilePath -> IO SSTable
loadSSTable path = do
  putStrLn $ "Loading SSTable from " ++ path
  withBinaryFile path ReadMode $ \h -> do
    hSeek h SeekFromEnd (-8)
    count <- runGet getWord64be <$> BL.hGet h 8
    putStrLn $ "  contains " ++ show count ++ " entries"
    -- Scanning entries
    hSeek h AbsoluteSeek 0
    idx <- buildIndex h (fromIntegral count) 0 []
    return SSTable {sstablePath = path, sstableIndex = V.fromList idx}
  where
    buildIndex :: Handle -> Int -> Int -> [(B.ByteString, Int64)] -> IO [(B.ByteString, Int64)]
    buildIndex _ 0 _ acc = return (reverse acc)
    buildIndex h n offset acc = do
      -- Read key length
      klenBs <- B.hGet h 4
      let klen = fromIntegral $ runGet getWord32be (BL.fromStrict klenBs)
      -- Read value length
      vlenBs <- B.hGet h 4
      let vlen = fromIntegral $ runGet getWord32be (BL.fromStrict vlenBs)
      -- Read key
      k <- B.hGet h klen
      -- Skip value
      _ <- B.hGet h vlen
      let newOffset = offset + 8 + klen + vlen
      buildIndex h (n - 1) newOffset ((k, fromIntegral offset) : acc)

lookupSSTables :: [SSTable] -> B.ByteString -> IO (Maybe B.ByteString)
lookupSSTables [] _ = return Nothing
lookupSSTables (sst : rest) k = do
  mv <- lookupSSTable sst k
  case mv of
    Just v -> pure $ if B.null v then Nothing else Just v
    Nothing -> lookupSSTables rest k

lookupSSTable :: SSTable -> B.ByteString -> IO (Maybe B.ByteString)
lookupSSTable sstable k = do
  putStrLn $ "Looking up key in SSTable: " ++ (show sstable) ++ " for key " ++ (show k)
  let indices = sstableIndex sstable
      mbIdx = binarySearch k indices
  case mbIdx of
    Just idx -> do
      let (_, offset) = indices V.! idx
      withBinaryFile (sstablePath sstable) ReadMode $ \h -> do
        putStrLn $ "  found at index " ++ show idx ++ " with offset " ++ show offset
        hSeek h AbsoluteSeek $ fromIntegral offset

        klenBs <- B.hGet h 4
        let klen = fromIntegral $ runGet getWord32be (BL.fromStrict klenBs)
        vlenBs <- B.hGet h 4
        let vlen = fromIntegral $ runGet getWord32be (BL.fromStrict vlenBs)

        key <- B.hGet h klen
        -- Check if the key matches
        if key /= k
          then trace ("key not found (deleted): " ++ show key) $ return Nothing
          else do
            if vlen == 0
              then return (Just B.empty) -- tombstone
              else Just <$> B.hGet h vlen
    Nothing -> return Nothing

binarySearch :: (Ord a) => a -> V.Vector (a, b) -> Maybe Int
binarySearch target xs = search 0 (V.length xs - 1)
  where
    search low high
      | low > high = Nothing -- Element not found
      | otherwise =
          let mid = low + (high - low) `div` 2
              (midVal, _) = xs V.! mid
           in case compare target midVal of
                LT -> search low (mid - 1) -- Target is in the lower half
                GT -> search (mid + 1) high -- Target is in the upper half
                EQ -> Just mid -- Target found at 'mid' index

encodeEntry :: B.ByteString -> B.ByteString -> BL.ByteString
encodeEntry k v = runPut $ do
  putWord32be (fromIntegral $ B.length k)
  putWord32be (fromIntegral $ B.length v)
  putByteString k
  putByteString v

sstName :: Int -> FilePath
sstName sid = "sst-" ++ pad sid ++ ".dat"
  where
    pad n = replicate (6 - length s) '0' ++ s
      where
        s = show n
