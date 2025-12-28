-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.Types where

import Control.Monad.Trans.Reader (ReaderT)
import qualified Data.ByteString as B
import Data.Int (Int64)
import qualified Data.Vector as V
import Internal.Fs (Fs, FsHandle)
import Internal.MemTable (MemTable)

data SSTable = SSTable
  { sstablePath :: FilePath,
    sstableIndex :: V.Vector (B.ByteString, Int64) -- (key, offset-of-entry)
  }
  deriving (Show)

data DB = DB
  { rootDir :: (FilePath),
    walH :: FsHandle,
    mem :: MemTable,
    ssts :: ![SSTable], -- newest first
    nextId :: !Int,
    memLimit :: !Int -- bytes rough limit
  }

type AppM = ReaderT Fs IO
