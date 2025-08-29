{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.SSTable where

import Control.Monad (forM)
import Data.Binary.Get (getWord32be, getWord64be, runGet)
import Data.Binary.Put (putByteString, putWord32be, putWord64be, runPut)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int64)
import qualified Data.Vector as V
import Debug.Trace (trace)
import System.FilePath ((</>))
import System.IO (Handle, IOMode (..), SeekMode (..), hFlush, hSeek, hTell, withBinaryFile)

data SSTable = SSTable
  { sstablePath :: FilePath,
    sstableIndex :: V.Vector (B.ByteString, Int64) -- (key, offset-of-entry)
  }
  deriving (Show)

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
