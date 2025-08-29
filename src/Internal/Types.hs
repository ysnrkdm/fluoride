-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.Types where

import qualified Data.ByteString as B
import qualified Data.Map.Strict as M
import Internal.SSTable (SSTable)
import System.IO (Handle)

data DB = DB
  { rootDir :: (FilePath),
    walH :: Handle,
    mem :: !(M.Map B.ByteString (Maybe B.ByteString)), -- Nothing=tombstone
    ssts :: ![SSTable], -- newest first
    nextId :: !Int,
    memLimit :: !Int, -- bytes rough limit
    memBytes :: !Int
  }
