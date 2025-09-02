{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.SSTable where

import Control.Monad (forM)
import Data.Binary.Get (getWord32be, getWord64be, runGet)
import Data.Binary.Put (putByteString, putWord32be, putWord64be, putWord8, runPut)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int64)
import qualified Data.Vector as V
import Debug.Trace (trace)
import qualified Internal.MemTable
import System.FilePath ((</>))
import System.IO (Handle, IOMode (..), SeekMode (..), hFlush, hSeek, hTell, withBinaryFile)

data SSTable = SSTable
  { sstablePath :: FilePath,
    sstableIndex :: V.Vector (B.ByteString, Int64) -- (key, offset-of-entry)
  }
  deriving (Show)

write :: FilePath -> Int -> [(Internal.MemTable.Op, B.ByteString, Maybe B.ByteString)] -> IO SSTable
write dir sid kvsAsc = do
  let path = dir </> sstName sid
  withBinaryFile path WriteMode $ \h -> do
    -- write entries
    _offsets <- forM kvsAsc $ \(op, k, v) -> do
      off <- hTell h
      B.hPut h $ BL.toStrict $ encodeEntry op k v
      return (k, off)
    B.hPut h $ BL.toStrict $ runPut $ putWord64be (fromIntegral $ length kvsAsc)
    hFlush h
    return ()
  load path

load :: FilePath -> IO SSTable
load path = do
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
      -- Read operation
      opLenBs <- B.hGet h 1
      let op = case B.unpack opLenBs of
            [1] -> Internal.MemTable.Put
            [2] -> Internal.MemTable.Del
            _ -> error "Invalid operation"
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
      let off = case op of
            Internal.MemTable.Put -> fromIntegral offset
            Internal.MemTable.Del -> -1
      buildIndex h (n - 1) newOffset ((k, off) : acc)

lookupChain :: [SSTable] -> B.ByteString -> IO (Maybe B.ByteString)
lookupChain [] _ = return Nothing
lookupChain (sst : rest) k = do
  mv <- lookupOne sst k
  case mv of
    Just v -> pure $ if B.null v then Nothing else Just v
    Nothing -> lookupChain rest k

lookupOne :: SSTable -> B.ByteString -> IO (Maybe B.ByteString)
lookupOne sstable k = do
  putStrLn $ "Looking up key in SSTable: " ++ (show sstable) ++ " for key " ++ (show k)
  let indices = sstableIndex sstable
      mbIdx = binarySearch k indices
  case mbIdx of
    Just idx -> do
      let (_, offset) = indices V.! idx
      withBinaryFile (sstablePath sstable) ReadMode $ \h -> do
        putStrLn $ "  found at index " ++ show idx ++ " with offset " ++ show offset
        hSeek h AbsoluteSeek $ fromIntegral offset

        opLenBs <- B.hGet h 1
        let op = case B.unpack opLenBs of
              [1] -> Internal.MemTable.Put
              [2] -> Internal.MemTable.Del
              _ -> error "Invalid operation"
        klenBs <- B.hGet h 4
        let klen = fromIntegral $ runGet getWord32be (BL.fromStrict klenBs)
        vlenBs <- B.hGet h 4
        let vlen = fromIntegral $ runGet getWord32be (BL.fromStrict vlenBs)

        key <- B.hGet h klen
        -- Check if the key matches
        if key /= k
          then trace ("key not found (deleted): " ++ show key) $ return Nothing
          else do
            case op of
              Internal.MemTable.Del -> return (Just B.empty) -- tombstone
              Internal.MemTable.Put -> Just <$> B.hGet h vlen
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

encodeEntry :: Internal.MemTable.Op -> B.ByteString -> Maybe B.ByteString -> BL.ByteString
encodeEntry Internal.MemTable.Put k (Just v) = runPut $ do
  putWord8 1
  putWord32be (fromIntegral $ B.length k)
  putWord32be (fromIntegral $ B.length v)
  putByteString k
  putByteString v
encodeEntry Internal.MemTable.Put _ Nothing =
  error "Put operation requires a value"
encodeEntry Internal.MemTable.Del _ (Just _) =
  error "Del operation should not have a value"
encodeEntry Internal.MemTable.Del k Nothing = runPut $ do
  putWord8 1
  putWord32be (fromIntegral $ B.length k)
  putWord32be 0
  putByteString k

sstName :: Int -> FilePath
sstName sid = "sst-" ++ pad sid ++ ".dat"
  where
    pad n = replicate (6 - length s) '0' ++ s
      where
        s = show n
