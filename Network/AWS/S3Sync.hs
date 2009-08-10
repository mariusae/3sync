module Network.AWS.S3Sync 
  ( diff, execute
  , Comparator(..)
  , Op(..)
  ) where

import Control.Monad (forM)
import Control.Exception (evaluate)

import Network.AWS.S3Bucket (ListRequest(..), listAllObjects, key, etag, size)
import Network.AWS.AWSConnection (AWSConnection(..), amazonS3ConnectionFromEnv)
import Network.AWS.S3Object (S3Object(..), getObject, sendObject, obj_data)
import qualified Network.Connection as Connection
import qualified Network.MiniHTTP.Client as Client
import qualified Network.MiniHTTP.URL as URL
import Network.MiniHTTP.HTTPConnection (hSource)
import Network.MiniHTTP.Marshal ( Headers(..)
                                , Request(..)
                                , Method(..)
                                , Reply(..)
                                , emptyHeaders 
                                , putRequest ) -- XXX

import Text.Printf (printf)
import Data.Digest.Pure.MD5 (md5)
import Data.String (fromString)
import Data.Maybe (fromJust)
import Data.Time.Clock (getCurrentTime)
import Data.Time.Format (formatTime)
import qualified Data.Set as S
import qualified Data.ByteString.Lazy as L

import qualified Data.ByteString as B -- XXX

import System.Posix.Files ( fileSize
                          , getSymbolicLinkStatus
                          , isSymbolicLink
                          , isDirectory
                          , getFileStatus
                          )
import System.Directory ( doesDirectoryExist
                        , getDirectoryContents
                        , createDirectoryIfMissing)
import System.FilePath ((</>), takeDirectory)
import System.IO (IOMode(..), openFile, hClose)
import System.Locale (TimeLocale(..))

import Network.AWS.S3 (SignData(..), signRequest)

import qualified Data.Binary.Put as P

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
-- | each direction using "Data.Set", producing a set of operations
-- | that need to be performed in order to synchronize the two.
diff :: Comparator -> FilePath -> String -> IO [Op]
diff comp dir bucket = do
  dents3 <- walkBucket comp bucket >>= return . S.fromList
  dents  <- walkDir comp dir       >>= return . S.fromList
  return $ (make Push $ dents S.\\ dents3) ++ (make Pull $ dents3 S.\\ dents)
  where
    make which = map ((\p -> which bucket (dir </> p) p) . fst) . S.toList

-- TODO: compute MD5 sums? This would be pertinent in terms of
-- security, since in our case, the MD5 of the uploaded object isn't
-- part of the signed string.
execute :: (Op -> IO a) -> [Op] -> IO ()
execute _ [] = return ()
execute report (op@(Push bucket local remote):ops) = do
  putFile bucket local remote

--   Just conn <- amazonS3ConnectionFromEnv
--   size <- getFileStatus local >>= return . fileSize
--   report op
--   contents <- L.readFile local
--   sendObject conn $ S3Object bucket 
--                              remote 
--                              ""
--                              [("Content-Length", show size)] 
--                              contents
  execute report ops

execute report (op@(Pull bucket local remote):ops) = do
  Just conn <- amazonS3ConnectionFromEnv
  report op
  createDirectoryIfMissing True $ takeDirectory local
  Right obj <- getObject conn $ S3Object bucket remote "" [] L.empty
  L.writeFile local (obj_data obj)
  execute report ops

-- | A more memory efficient file uploader. This uses
-- | "Network.MiniHTTP" so that we can stream the file via a data
-- | source, avoiding using as much memory as the file is big.
putFile bucket local remote = do
  Just awsConn <- amazonS3ConnectionFromEnv

  -- Construct a URL and the appropriate headers to use.
  let Just url = URL.parse 
               $ fromString 
               $ printf "http://%s.s3.amazonaws.com/%s" bucket remote

  currentTime <- getCurrentTime

  -- Sign the request
  let signed = signRequest 
               SignData 
               { accessKey       = awsAccessKey awsConn
               , secretAccessKey = awsSecretKey awsConn
               , httpVerb        = "PUT"
               , contentMD5      = ""
               , contentType     = ""
               , date            = formatTime timeLocale 
                                   "%a, %d %b %Y %H:%M:%S GMT" currentTime
               , bucket          = bucket
               , resource        = remote
               }

  -- Construct the HTTP headers.
  size <- getFileStatus local >>= return . toInteger . fileSize
  let headers = emptyHeaders {
      httpDate          = Just currentTime
    , httpContentLength = Just $ fromInteger $ size
    , httpHost          = Just $ fromString $ printf "%s.s3.amazonaws.com" bucket
    , httpAuthorization = Just $ fromString 
                               $ printf "AWS %s:%s" (awsAccessKey awsConn) signed
    }

  -- Finally ready, so make a connection.
  conn <- Client.connection url >>= Client.transport url

  let request = (Request PUT (URL.toRelative url) 1 1 headers)

  -- The source is the file.
  h      <- openFile local ReadMode
  source <- hSource (0, fromInteger (size - 1)) h

  r <- Client.request conn request (Just source)
  case r of 
    Just (reply, _) | replyStatus reply == 200 -> do
      putStrLn $ show reply
      Connection.close conn
      hClose h
    _ -> fail "Request error."

  return ()

  where
    -- | Copy/paste of "Network.MiniHTTP.Marshal", to make sure we can
    -- | replicate the @Date@ header.
    timeLocale = TimeLocale 
                 { wDays = [ ("Sunday", "Sun")
                           , ("Monday", "Mon")
                           , ("Tuesday", "Tue")
                           , ("Wednesday", "Wed")
                           , ("Thursday", "Thu")
                           , ("Friday", "Fri")
                           , ("Saturday", "Sat")
                           ]
                 , months = [ ("January", "Jan")
                            , ("February", "Feb")
                            , ("March", "Mar")
                            , ("April", "Apr")
                            , ("May", "May")
                            , ("June", "Jun")
                            , ("July", "Jul")
                            , ("August", "Aug")
                            , ("September", "Sep")
                            , ("October", "Oct")
                            , ("November", "Nov")
                            , ("December", "Dec")
                            ]
                 , intervals = [ ("year", "years")
                               , ("month", "months")
                               , ("day", "days")
                               , ("hour", "hours")
                               , ("min", "mins")
                               , ("sec", "secs")
                               , ("usec", "usecs")
                               ]
                 , amPm = ("AM", "PM")
                 , dateTimeFmt = "%a %b %e %H:%M:%S %Z %Y"
                 , dateFmt = "%m/%d/%y"
                 , timeFmt = "%H:%M:%S"
                 , time12Fmt = "%I:%M:%S %p"
                 }
