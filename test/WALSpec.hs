{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module WALSpec (walTests) where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Reader (ask, runReaderT)
import Control.Exception (SomeException, evaluate, try)
import Internal.Fs (Fs (..), newMemFs)
import qualified Internal.MemTable as MemTable
import qualified Internal.WAL as WAL
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (testCase, (@?=))

walTests :: TestTree
walTests =
  testGroup
    "WAL"
    [ testCase "append and recover" $ do
        fs <- newMemFs
        let walPath = "wal.log"
        runReaderT
          ( do
            Fs {..} <- ask
            h <- liftIO $ fsOpenRW walPath
            WAL.append h 0 "k1" (Just "v1")
            WAL.append h 1 "k1" Nothing
            liftIO $ fsClose h
          )
          fs
        mem <- runReaderT (WAL.recover walPath) fs
        MemTable.lookup "k1" mem @?= Just Nothing
    , testCase "recover non-existent WAL yields empty MemTable" $ do
        fs <- newMemFs
        mem <- runReaderT (WAL.recover "no-wal.log") fs
        MemTable.isEmpty mem @?= True
    , testCase "recover with multiple keys and updates" $ do
        fs <- newMemFs
        let walPath = "wal2.log"
        runReaderT
          ( do
            Fs {..} <- ask
            h <- liftIO $ fsOpenRW walPath
            WAL.append h 0 "k1" (Just "v1")
            WAL.append h 0 "k2" (Just "v2")
            WAL.append h 0 "k1" (Just "v1-updated")
            WAL.append h 1 "k2" Nothing
            liftIO $ fsClose h
          )
          fs
        mem <- runReaderT (WAL.recover walPath) fs
        MemTable.lookup "k1" mem @?= Just (Just "v1-updated")
        MemTable.lookup "k2" mem @?= Just Nothing,
      testCase "recover empty WAL file" $ do
        fs <- newMemFs
        let walPath = "empty-wal.log"
        runReaderT
          ( do
              Fs {..} <- ask
              h <- liftIO $ fsOpenRW walPath
              liftIO $ fsClose h
          )
          fs
        mem <- runReaderT (WAL.recover walPath) fs
        MemTable.isEmpty mem @?= True,
      testCase "recover on truncated WAL yields empty or fails gracefully" $ do
        fs <- newMemFs
        let walPath = "truncated.log"
        -- write incomplete record (just op byte without lengths/body)
        runReaderT
          ( do
              Fs {..} <- ask
              h <- liftIO $ fsOpenRW walPath
              -- op byte only, missing lengths
              liftIO $ fsWrite h "\0"
              liftIO $ fsClose h
          )
          fs
        res <- (try @SomeException) $ runReaderT (WAL.recover walPath >>= liftIO . evaluate) fs
        case res of
          Left _ -> pure ()
          Right mem -> MemTable.isEmpty mem @?= True
    ]
