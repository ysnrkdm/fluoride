{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.MemTable where

import qualified Data.ByteString as B
import qualified Data.Map.Strict as M

data Op = Put | Del
  deriving (Show, Eq)

data MemTable = MT
  { m :: M.Map B.ByteString (Maybe B.ByteString),
    bytes :: !Int -- approx size in bytes
  }

empty :: MemTable
empty = MT M.empty 0

isEmpty :: MemTable -> Bool
isEmpty (MT m _) = M.null m

sizeBytes :: MemTable -> Int
sizeBytes (MT _ bytes) = bytes

isExceedingSizeBytes :: MemTable -> Int -> Bool
isExceedingSizeBytes (MT _ bytes) limit = bytes > limit

lookup :: B.ByteString -> MemTable -> Maybe (Maybe B.ByteString)
lookup k (MT m _) = M.lookup k m

bytesOfEntry :: B.ByteString -> Maybe B.ByteString -> Int
bytesOfEntry k mv = 9 + B.length k + maybe 0 B.length mv

insert :: B.ByteString -> B.ByteString -> MemTable -> MemTable
insert k v (MT m bytes) =
  MT (M.insert k (Just v) m) (bytes + bytesOfEntry k (Just v))

delete :: B.ByteString -> MemTable -> MemTable
delete k (MT m bytes) =
  let delta = bytesOfEntry k Nothing
   in MT (M.insert k Nothing m) (bytes + delta)

snapshotAsc :: MemTable -> [(Op, B.ByteString, Maybe B.ByteString)]
snapshotAsc (MT m _) =
  [(Put, k, Just v) | (k, Just v) <- M.toAscList m]
    ++ [(Del, k, Nothing) | (k, Nothing) <- M.toAscList m]
