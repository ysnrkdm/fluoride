{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.SSTable where

import Control.Monad (forM)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Reader (ask)
import Data.Binary.Get (getWord32be, getWord64be, runGet)
import Data.Binary.Put (putByteString, putWord32be, putWord64be, putWord8, runPut)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int64)
import qualified Data.Vector as V
import Internal.Fs (Fs (..), FsHandle)
import qualified Internal.MemTable
import Internal.Types (AppM, SSTable (..))
import System.FilePath ((</>))
import System.IO (SeekMode (..))

write :: FilePath -> Int -> [(Internal.MemTable.Op, B.ByteString, Maybe B.ByteString)] -> AppM SSTable
write dir sid kvsAsc = do
  Fs {..} <- ask
  let path = dir </> sstName sid
  h <- liftIO $ fsOpenRW path
  liftIO $ do
    -- write entries
    _offsets <- forM kvsAsc $ \(op, k, v) -> do
      off <- fsTell h
      fsWrite h $ BL.toStrict $ encodeEntry op k v
      return (k, off)
    fsWrite h $ BL.toStrict $ runPut $ putWord64be (fromIntegral $ length kvsAsc)
    fsFlush h
    fsSync h
    fsClose h
  load path

load :: FilePath -> AppM SSTable
load path = do
  Fs {..} <- ask
  liftIO $ putStrLn $ "Loading SSTable from " ++ path
  h <- liftIO $ fsOpenRO path
  liftIO $ fsSeek h SeekFromEnd (-8)
  countBs <- liftIO $ fsRead h 8
  let count = runGet getWord64be (BL.fromStrict countBs)
  liftIO $ putStrLn $ "  contains " ++ show count ++ " entries"
  liftIO $ fsSeek h AbsoluteSeek 0
  idx <- liftIO $ buildIndex fsRead h (fromIntegral count) 0 []
  liftIO $ fsClose h
  return SSTable {sstablePath = path, sstableIndex = V.fromList idx}
  where
    buildIndex :: (FsHandle -> Int -> IO B.ByteString) -> FsHandle -> Int -> Integer -> [(B.ByteString, Int64)] -> IO [(B.ByteString, Int64)]
    buildIndex _ _ 0 _ acc = return (reverse acc)
    buildIndex reader h n offset acc = do
      -- Read operation
      opLenBs <- reader h 1
      let op = case B.unpack opLenBs of
            [1] -> Internal.MemTable.Put
            [2] -> Internal.MemTable.Del
            _ -> error "Invalid operation"
      -- Read key length
      klenBs <- reader h 4
      let klen = fromIntegral $ runGet getWord32be (BL.fromStrict klenBs)
      -- Read value length
      vlenBs <- reader h 4
      let vlen = fromIntegral $ runGet getWord32be (BL.fromStrict vlenBs)
      -- Read key
      k <- reader h klen
      -- Skip value
      _ <- reader h vlen
      let newOffset = (fromIntegral offset) + 8 + klen + vlen
      let off = case op of
            Internal.MemTable.Put -> fromIntegral offset
            Internal.MemTable.Del -> -1
      buildIndex reader h (n - 1) (fromIntegral newOffset) ((k, off) : acc)

lookupChain :: [SSTable] -> B.ByteString -> AppM (Maybe B.ByteString)
lookupChain [] _ = return Nothing
lookupChain (sst : rest) k = do
  mv <- lookupOne sst k
  case mv of
    Just v -> pure $ if B.null v then Nothing else Just v
    Nothing -> lookupChain rest k

lookupOne :: SSTable -> B.ByteString -> AppM (Maybe B.ByteString)
lookupOne sstable k = do
  Fs {..} <- ask
  liftIO $ putStrLn $ "Looking up key in SSTable: " ++ show sstable ++ " for key " ++ show k
  let indices = sstableIndex sstable
      mbIdx = binarySearch k indices
  case mbIdx of
    Just idx -> do
      let (_, offset) = indices V.! idx
      h <- liftIO $ fsOpenRO (sstablePath sstable)
      liftIO $ putStrLn $ "  found at index " ++ show idx ++ " with offset " ++ show offset
      liftIO $ fsSeek h AbsoluteSeek $ fromIntegral offset

      opLenBs <- liftIO $ fsRead h 1
      let op = case B.unpack opLenBs of
            [1] -> Internal.MemTable.Put
            [2] -> Internal.MemTable.Del
            _ -> error "Invalid operation"
      klenBs <- liftIO $ fsRead h 4
      let klen = fromIntegral $ runGet getWord32be (BL.fromStrict klenBs)
      vlenBs <- liftIO $ fsRead h 4
      let vlen = fromIntegral $ runGet getWord32be (BL.fromStrict vlenBs)

      key <- liftIO $ fsRead h klen
      mv <-
        if key /= k
          then return Nothing
          else case op of
            Internal.MemTable.Del -> return (Just B.empty) -- tombstone
            Internal.MemTable.Put -> Just <$> liftIO (fsRead h vlen)
      liftIO $ fsClose h
      return mv
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
