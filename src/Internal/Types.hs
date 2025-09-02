-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.Types where

import Internal.MemTable (MemTable)
import Internal.SSTable (SSTable)
import System.IO (Handle)

data DB = DB
  { rootDir :: (FilePath),
    walH :: Handle,
    mem :: MemTable,
    ssts :: ![SSTable], -- newest first
    nextId :: !Int,
    memLimit :: !Int -- bytes rough limit
  }
