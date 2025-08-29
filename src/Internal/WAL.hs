{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.WAL where

import Data.Binary.Get (getWord32be, runGet)
import Data.Binary.Put (putByteString, putWord32be, putWord8, runPut)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import Data.Word (Word8)
import System.Directory (doesFileExist)
import System.IO (Handle, hFlush)

append :: Handle -> Word8 -> B.ByteString -> Maybe B.ByteString -> IO ()
append h op k mv = do
  let bs = runPut $ do
        putWord8 op
        putWord32be (fromIntegral $ B.length k)
        putWord32be (fromIntegral $ maybe 0 B.length mv)
        putByteString k
        maybe (pure ()) putByteString mv
  BL.hPut h bs >> hFlush h

recover :: FilePath -> IO (M.Map B.ByteString (Maybe B.ByteString))
recover wal = do
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
