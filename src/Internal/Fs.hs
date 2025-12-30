{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RecordWildCards #-}
-- since this is Internal, expose everything
{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module Internal.Fs where

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import qualified Data.Map.Strict as Map
import System.Directory (createDirectoryIfMissing, doesFileExist, listDirectory)
import System.FilePath (takeDirectory, takeFileName)
import System.IO (Handle, IOMode (WriteMode), SeekMode (..), hClose, hFlush, hSeek, hTell, withFile)
import Foreign.C.Error (throwErrnoIfMinus1_)
import Foreign.C.Types (CInt (..))
import System.Posix.IO (OpenMode (..), OpenFileFlags (..), defaultFileFlags, fdToHandle, openFd)
import System.Posix.Types (Fd (..))

data Fs = Fs
  { fsOpenRO :: FilePath -> IO FsHandle,
    fsOpenRW :: FilePath -> IO FsHandle,
    fsClose :: FsHandle -> IO (),
    fsRead :: FsHandle -> Int -> IO ByteString,
    fsWrite :: FsHandle -> ByteString -> IO (),
    fsSeek :: FsHandle -> SeekMode -> Integer -> IO (),
    fsTell :: FsHandle -> IO Integer,
    fsFlush :: FsHandle -> IO (),
    fsSync :: FsHandle -> IO (),
    fsListDir :: FilePath -> IO [FilePath],
    fsEnsureDir :: FilePath -> IO (),
    fsExists :: FilePath -> IO Bool,
    fsTruncate :: FilePath -> IO (),
    fsReadAll :: FilePath -> IO ByteString
  }

data FsHandle
  = FsHandleIO Handle Fd
  | FsHandleMem MemHandle

foreign import ccall unsafe "fsync" c_fsync :: CInt -> IO CInt

data MemHandle = MemHandle
  { mhPath :: FilePath,
    mhPos :: IORef Integer,
    mhFs :: MemFs
  }

data MemFs = MemFs
  { mfFiles :: IORef (Map.Map FilePath ByteString)
  }

ioFs :: Fs
ioFs =
  Fs
    { fsOpenRO = openFileRead,
      fsOpenRW = openFileReadWrite,
      fsClose = \case
        FsHandleIO h _ -> hClose h
        FsHandleMem _ -> pure (),
      fsRead = \case
        FsHandleIO h _ -> \n -> B.hGet h n
        FsHandleMem mh -> memRead mh,
      fsWrite = \case
        FsHandleIO h _ -> \bs -> B.hPut h bs
        FsHandleMem mh -> memWrite mh,
      fsSeek = \case
        FsHandleIO h _ -> hSeek h
        FsHandleMem mh -> memSeek mh,
      fsTell = \case
        FsHandleIO h _ -> hTell h
        FsHandleMem mh -> memTell mh,
      fsFlush = \case
        FsHandleIO h _ -> hFlush h
        FsHandleMem _ -> pure (),
      fsSync = \case
        FsHandleIO h (Fd fd) -> hFlush h >> throwErrnoIfMinus1_ "fsync" (c_fsync fd)
        FsHandleMem _ -> pure (),
      fsListDir = listDirectory,
      fsEnsureDir = \p -> createDirectoryIfMissing True p,
      fsExists = doesFileExist,
      fsTruncate = \p -> withFile p WriteMode (\_ -> pure ()),
      fsReadAll = B.readFile
    }
  where
    openFileRead p = do
      fd <- openFd p ReadOnly defaultFileFlags
      h <- fdToHandle fd
      pure (FsHandleIO h fd)
    openFileReadWrite p = do
      fd <- openFd p ReadWrite defaultFileFlags {creat = Just 0o644}
      h <- fdToHandle fd
      pure (FsHandleIO h fd)

newMemFs :: IO Fs
newMemFs = do
  mfFiles <- newIORef Map.empty
  let memFs = MemFs {mfFiles}
  pure $
    Fs
      { fsOpenRO = memOpenRO memFs,
        fsOpenRW = memOpenRW memFs,
        fsClose = memOnly "fsClose" (const $ pure ()),
        fsRead = memOnly "fsRead" memRead,
        fsWrite = memOnly "fsWrite" memWrite,
        fsSeek = memOnly "fsSeek" memSeek,
        fsTell = memOnly "fsTell" memTell,
        fsFlush = memOnly "fsFlush" (const $ pure ()),
        fsSync = memOnly "fsSync" (const $ pure ()),
        fsListDir = memListDir memFs,
        fsEnsureDir = const (pure ()),
        fsExists = memExists memFs,
        fsTruncate = memTruncate memFs,
        fsReadAll = memReadAll memFs
      }
  where
    memOnly :: String -> (MemHandle -> r) -> FsHandle -> r
    memOnly label f = \case
      FsHandleMem mh -> f mh
      FsHandleIO _ _ -> error $ label ++ ": FsHandleIO in mem fs"

memOpenRO :: MemFs -> FilePath -> IO FsHandle
memOpenRO fs path = do
  files <- readIORef (mfFiles fs)
  case Map.lookup path files of
    Nothing -> ioError (userError $ "fsOpenRO: file not found: " ++ path)
    Just _ -> FsHandleMem <$> newMemHandle fs path

memOpenRW :: MemFs -> FilePath -> IO FsHandle
memOpenRW fs path = do
  modifyIORef' (mfFiles fs) (Map.alter (\v -> case v of Nothing -> Just B.empty; Just bs -> Just bs) path)
  FsHandleMem <$> newMemHandle fs path

memRead :: MemHandle -> Int -> IO ByteString
memRead MemHandle {..} n = do
  pos <- readIORef mhPos
  files <- readIORef (mfFiles mhFs)
  let content = Map.findWithDefault B.empty mhPath files
      start = pos
      slice = B.take n $ B.drop (fromIntegral start) content
  writeIORef mhPos (pos + fromIntegral (B.length slice))
  pure slice

memWrite :: MemHandle -> ByteString -> IO ()
memWrite MemHandle {..} bs = do
  pos <- readIORef mhPos
  let posInt = fromIntegral pos :: Int
      lenBs = B.length bs
  modifyIORef' (mfFiles mhFs) $ \m ->
    let content = Map.findWithDefault B.empty mhPath m
        contentLen = B.length content
        prefix =
          if posInt <= contentLen
            then B.take posInt content
            else content <> B.replicate (posInt - contentLen) 0
        suffixStart = posInt + lenBs
        suffix =
          if suffixStart < contentLen
            then B.drop suffixStart content
            else B.empty
        newContent = prefix <> bs <> suffix
     in Map.insert mhPath newContent m
  writeIORef mhPos (pos + fromIntegral lenBs)

memSeek :: MemHandle -> SeekMode -> Integer -> IO ()
memSeek MemHandle {..} mode off = do
  pos <- readIORef mhPos
  files <- readIORef (mfFiles mhFs)
  let content = Map.findWithDefault B.empty mhPath files
      size = fromIntegral (B.length content) :: Integer
      base = case mode of
        AbsoluteSeek -> 0
        RelativeSeek -> pos
        SeekFromEnd -> size
      newPos = max 0 (base + off)
  writeIORef mhPos newPos

memTell :: MemHandle -> IO Integer
memTell MemHandle {..} = readIORef mhPos

memListDir :: MemFs -> FilePath -> IO [FilePath]
memListDir MemFs {..} dir = do
  files <- readIORef mfFiles
  let names = [takeFileName p | p <- Map.keys files, takeDirectory p == dir]
  pure names

memExists :: MemFs -> FilePath -> IO Bool
memExists MemFs {..} path = Map.member path <$> readIORef mfFiles

memTruncate :: MemFs -> FilePath -> IO ()
memTruncate MemFs {..} path =
  modifyIORef' mfFiles (Map.insert path B.empty)

memReadAll :: MemFs -> FilePath -> IO ByteString
memReadAll MemFs {..} path = Map.findWithDefault B.empty path <$> readIORef mfFiles

newMemHandle :: MemFs -> FilePath -> IO MemHandle
newMemHandle fs path = do
  mhPos <- newIORef 0
  pure MemHandle {mhPath = path, mhPos, mhFs = fs}
