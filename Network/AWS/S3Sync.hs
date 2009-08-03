module Network.AWS.S3Sync (
    diff
  ) where

import Control.Monad

import System.Posix.Files
import Network.AWS.S3Bucket
import Network.AWS.AWSConnection
import Network.AWS.AWSResult
import Network.AWS.S3Object

import Data.List
import Data.Digest.Pure.MD5
import qualified Data.ByteString.Lazy as L
import Data.Digest.Pure.MD5
import System.Directory (doesDirectoryExist, getDirectoryContents)
import System.FilePath ((</>))

concatMapM f xs = mapM f xs >>= return . concat

walkDir dir =
  walkEnt True dir >>= mapM addMD5
  where
    addMD5 ent = do
      f <- L.readFile $ dir </> ent
      return $ (ent, show $ md5 f)

    walkEnt isRoot ent = do
      fs <- getSymbolicLinkStatus ent'
      if isSymbolicLink fs
         then return []
         else if isDirectory fs
           then getDirectoryContents ent' >>= 
                return . filter (`notElem` [".", ".."]) >>= 
                concatMapM (walkEnt False) . map addDir
           else
             return [ent]
      where
        ent' = if isRoot then ent else dir </> ent
        addDir = if isRoot then id else (</>) ent

walkBucket bucket = do
  Just conn  <- amazonS3ConnectionFromEnv
  Right objs <- listObjects conn bucket (ListRequest "" "" "" 1000)
  forM (snd objs) $ \obj -> return $ ((key obj), (etag obj))

diff dir bucket = do
  dents <- walkDir dir
  dents3 <- walkBucket bucket

  return $ dents \\ dents3

  