module Network.AWS.S3Sync (
    diff, execute
  , Comparator(..)
  , Op(..)
  ) where

import Control.Monad
import Control.Exception(evaluate)

import Network.AWS.S3Bucket
import Network.AWS.AWSConnection
import Network.AWS.AWSResult
import Network.AWS.S3Object

import Text.Printf
import Data.List
import Data.Digest.Pure.MD5
import qualified Data.Set as S
import qualified Data.ByteString.Lazy as L

import System.Posix.Files
import System.Directory ( doesDirectoryExist
                        , getDirectoryContents
                        , createDirectoryIfMissing)
import System.FilePath ((</>), takeDirectory)

-- Strange that something like this isn't in the prelude?
concatMapM f xs = mapM f xs >>= return . concat

-- | Comparators specify modes of file comparison.
data Comparator = Size | MD5

-- Operations specify local directory, bucket name, and bucket path
data Op = Push String String String
        | Pull String String String
          deriving (Eq, Ord)

instance Show Op where
  show (Push b l r) = printf "%s => %s/%s" l b r
  show (Pull b l r) = printf "%s <= %s/%s" l b r

-- | Walk a local directory, applying the comparator.
walkDir comp dir =
  -- feels like an Arrow to me..
  walkEnt True dir >>= mapM (addComp comp)
  where
    addComp MD5 (ent, _) = do
      f <- L.readFile $ dir </> ent
      checksum <- evaluate $ show $ md5 f
      return (ent, checksum)
    addComp Size (ent, fs) = 
      return $ (ent, show $ fromIntegral $ fileSize fs)

    walkEnt isRoot ent = do
      fs <- getSymbolicLinkStatus ent'
      if isSymbolicLink fs
         then return []
         else if isDirectory fs
           then getDirectoryContents ent' >>= 
                return . filter (`notElem` [".", ".."]) >>= 
                concatMapM (walkEnt False) . map addDir
              else
             return [(ent, fs)]
      where
        ent' = if isRoot then ent else dir </> ent
        addDir = if isRoot then id else (</>) ent

-- | Walk an S3 bucket, applying the comparator.
walkBucket comp bucket = do
  Just conn  <- amazonS3ConnectionFromEnv
  Right objs <- listAllObjects conn bucket (ListRequest "" "" "" 1000000)
  forM objs $ \obj -> return $ (key obj, compKey comp obj)
  where
    compKey MD5  = etag
    compKey Size = show . size

-- | Walk the bucket & the directory, then compute the differences in
-- | each direction using @Data.Set@, producing a set of operations
-- | that need to be performed in order to synchronize the two.
diff comp dir bucket = do
  dents3 <- walkBucket comp bucket >>= return . S.fromList
  dents  <- walkDir comp dir       >>= return . S.fromList
  return $ (make Push $ dents S.\\ dents3) ++ (make Pull $ dents3 S.\\ dents)
  where
    make which = map ((\p -> which bucket (dir </> p) p) . fst) . S.toList


execute _ [] = return ()
execute report (op@(Push bucket local remote):ops) = do
  Just conn <- amazonS3ConnectionFromEnv
  -- TODO: really this should be a logger, maybe run inside a monad
  size <- getFileStatus local >>= return . fileSize
  report op
  contents <- L.readFile local
  sendObject conn $ S3Object bucket remote "" [("Content-Length", show size)] contents
  execute report ops

execute report (op@(Pull bucket local remote):ops) = do
  Just conn <- amazonS3ConnectionFromEnv
  report op
  createDirectoryIfMissing True $ takeDirectory local
  Right obj <- getObject conn $ S3Object bucket remote "" [] L.empty
  L.writeFile local (obj_data obj)
  execute report ops
