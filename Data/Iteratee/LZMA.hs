{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns #-}
{-# OPTIONS_GHC -Wall #-}
module Data.Iteratee.LZMA
  ( -- * Enumeratees
    enumDecompress
  , enumDecompressRandom

  -- * Parameters
  , DecompressParams, defaultDecompressParams
  , decompressBufferSize
  , decompressMemoryLimit

  -- * Utils
  , isCompressed
  , decodeIndex
  ) where
import Control.Exception
import Control.Monad.Trans

import Data.ByteString (ByteString)
import Data.Iteratee
import Pipes (hoist)
import qualified Data.ByteString as S
import qualified Pipes.Internal as P

import Codec.Compression.LZMA.Incremental hiding (decodeIndex)

-- | Decompress the input and send to inner iteratee. If there is end of LZMA
-- stream it is left unprocessed.
enumDecompress
    :: forall m a. MonadIO m
    => DecompressParams
    -> Enumeratee ByteString ByteString m a
enumDecompress = go . decompressIO
    where
        go :: DecompressStream IO r
            -> Enumeratee ByteString ByteString m a
        go (P.Request () feed) inner = do
            compressed <- getChunk
            go (feed compressed) inner
        go (P.Respond uncompressed demand) inner = do
            inner' <- lift $ enumPure1Chunk uncompressed inner
            go (demand ()) inner'
        go (P.M m) inner = do
            stream <- liftIO m
            go stream inner
        go (P.Pure _) inner = return inner

-- | Same as 'enumDecompress' but supports random seek.
enumDecompressRandom
    :: forall m a. MonadIO m
    => Index
    -> DecompressParams
    -> Enumeratee ByteString ByteString m a
enumDecompressRandom index params =
    go $ seekableDecompressIO params index Read
    where
        go :: SeekableDecompressStream IO r
            -> Enumeratee ByteString ByteString m a
        go (P.Request req feed) inner = do
            case req :: ReadRequest 'Compressed of
                PRead pos -> seek (fromIntegral pos)
                Read -> return ()
            compressed <- getChunk
            go (feed compressed) inner
        go (P.Respond uncompressed demand) inner = do
            -- the inner iteratee works on raw stream rather than compressed
            -- stream.
            (inner', req'm) <- lift $ runIter inner
                (\a s -> return (idone a s, Nothing))
                (\k e -> return $ onCont k e)
            case req'm of
                Nothing -> return inner'
                Just req -> go (demand req) inner'
            where
                onCont k Nothing =
                    (k $ Chunk uncompressed, Just Read)
                onCont k (Just (fromException -> Just (SeekException pos))) =
                    (k $ Chunk S.empty, Just $ PRead $ fromIntegral pos)
                onCont _ (Just e) =
                    (throwErr e, Nothing)
        go (P.M m) inner = do
            stream <- liftIO m
            go stream inner
        go (P.Pure _) inner = do
            -- Even if the decompressor reached to the end, the inner iteratee
            -- could demand more input (e.g. rewind the position after it
            -- reached to the end). So we need to check the inner iteratee
            -- first to see if it's asking for a new position.
            (inner', req'm) <- lift $ runIter inner
                (\a s -> return (idone a s, Nothing))
                (\k e -> return $ onCont k e)
            case req'm of
                Nothing -> return inner'
                Just req -> go (seekableDecompressIO params index req) inner'
            where
                -- Check the iteratee's state by supplying EOF.
                -- If the iteratee is asking for a position, we restart the
                -- decompress from the position. Otherwise we just continue
                -- from the current position.
                onCont k (Just (fromException -> Just (SeekException pos))) =
                    (k $ EOF Nothing, Just $ PRead $ fromIntegral pos)
                onCont k e =
                    (k $ EOF e, Just Read)

isCompressed :: forall m. MonadIO m => Iteratee ByteString m Bool
isCompressed = seek 0 >> decodeToIteratee hasMagicBytes

decodeIndex :: MonadIO m => FileOffset -> Iteratee ByteString m Index
decodeIndex =
    fmap fst . decodeToIteratee . hoist liftIO . decodeIndexIO . fromIntegral

decodeToIteratee :: Monad m => DecodeStream m a -> Iteratee ByteString m a
decodeToIteratee = go
    where
        go (P.Request seekRequest next) = do
            case seekRequest of
                PRead pos -> seek $ fromIntegral pos
                Read -> return ()
            chunk <- getChunk
            go $ next chunk
        go (P.Respond _ next) = go $ next ()
        go (P.M m) = lift m >>= go
        go (P.Pure a) = return a
