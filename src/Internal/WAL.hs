{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.WAL where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Reader (ask)
import Data.Binary.Get (getWord32be, runGet)
import Data.Binary.Put (putByteString, putWord32be, putWord8, runPut)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Word (Word8)
import Internal.Fs (Fs (..), FsHandle)
import Internal.MemTable (MemTable)
import qualified Internal.MemTable as MemTable
import Internal.Types (AppM)

append :: FsHandle -> Word8 -> B.ByteString -> Maybe B.ByteString -> AppM ()
append h op k mv = do
  Fs {..} <- ask
  let bs = BL.toStrict $
        runPut $ do
          putWord8 op
          putWord32be (fromIntegral $ B.length k)
          putWord32be (fromIntegral $ maybe 0 B.length mv)
          putByteString k
          maybe (pure ()) putByteString mv
  liftIO $ fsWrite h bs >> fsFlush h

recover :: FilePath -> AppM MemTable
recover wal = do
  Fs {..} <- ask
  exists <- liftIO $ fsExists wal
  if not exists
    then return MemTable.empty
    else do
      contents <- liftIO $ fsReadAll wal
      return (go MemTable.empty contents)
  where
    go :: MemTable -> B.ByteString -> MemTable
    go m s
      | B.null s = m
      | otherwise =
          let (op, s1) = B.splitAt 1 s
              (klenBs, s2) = get32 s1
              (vlenBs, s3) = get32 s2
              (k, s4) = B.splitAt (fromIntegral klenBs) s3
              (v, rest) = B.splitAt (fromIntegral vlenBs) s4
              m' = case B.head op of
                0 -> MemTable.insert k v m
                1 -> MemTable.delete k m
                _ -> error "corrupted WAL"
           in go m' rest

    get32 t =
      let (bs, rest) = B.splitAt 4 t
          x = runGet getWord32be (BL.fromStrict bs)
       in (x, rest)
