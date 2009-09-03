{-# LANGUAGE DeriveDataTypeable #-}

module Network.AWS.S3Sync 
  ( diff, execute, hPut
  , Comparator(..)
  , Op(..)
  , Report(..)
  ) where

import           Control.Monad (forM)
import           Control.Exception (Exception(..), evaluate, throw)

import           Data.Digest.Pure.MD5 (md5)
import           Data.String (fromString)
import           Data.Time.Clock (getCurrentTime)
import           Data.Time.Format (formatTime)
import           Data.Typeable (Typeable(..))
import           Data.Maybe (maybe)
import           Text.Printf (printf)
import qualified Data.Set as S
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString as B

import qualified Network.Connection as Connection
import           System.Posix.Files ( fileSize
                                    , getSymbolicLinkStatus
                                    , isSymbolicLink
                                    , isDirectory
                                    , getFileStatus
                                    )
import           System.Directory ( getDirectoryContents
                                  , createDirectoryIfMissing)
import           System.FilePath ((</>), takeDirectory)
import           System.IO (IOMode(..), Handle, withFile, hIsEOF)
import           System.Locale (TimeLocale(..))

import qualified Network.MiniHTTP.Client as Client
import qualified Network.MiniHTTP.URL as URL
import           Network.MiniHTTP.HTTPConnection (Source(..), SourceResult(..))
import           Network.MiniHTTP.Marshal ( Headers(..)
                                          , Request(..)
                                          , Method(..)
                                          , Reply(..)
                                          , emptyHeaders
                                          )

import           Network.AWS.S3Bucket ( ListRequest(..)
                                      , listAllObjects
                                      , key
                                      , etag
                                      , size
                                      )
import           Network.AWS.AWSConnection ( AWSConnection(..)
                                           , amazonS3ConnectionFromEnv
                                           )
import           Network.AWS.S3Object (S3Object(..), getObject)
import           Network.AWS.S3 (SignData(..), signRequest)

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

data S3Exception = HttpError String
                   deriving (Show, Typeable)

data Report = Transferred Integer
            | Done

instance Exception S3Exception

-- | Walk a local directory, applying the comparator.
walkDir comp dir =
  -- feels like an Arrow to me..
  walkEnt True dir >>= mapM (addComp comp)
  where
    addComp MD5 (ent, _) = do
      f <- L.readFile $ dir </> ent
      -- We evaluate here in order to ensure that we don't sit with
      -- too many filehandles. Though this may result in too much
      -- eagerness in MD5 checksum computation, so a better solution
      -- would involve delaying the opening of the file until needed
      -- (eg. if readFile did not need to actually perform the open..)
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
execute :: (Op -> Integer -> IO (Report -> IO())) -> [Op] -> IO ()
execute _ [] = return ()

execute report (op@(Push bucket local remote):ops) = do
  size <- getFileStatus local >>= return . Just . toInteger . fileSize
  report' <- report op $ maybe 0 id size
  withFile local ReadMode $ \h -> hPut h size bucket remote report'
  execute report ops

execute report (op@(Pull bucket local remote):ops) = do
  Just conn <- amazonS3ConnectionFromEnv
  createDirectoryIfMissing True $ takeDirectory local
  withFile local WriteMode $ \h -> hGet h bucket remote (report op)
  -- Right obj <- getObject conn $ S3Object bucket remote "" [] L.empty
  -- L.writeFile local (obj_data obj)
  execute report ops

signedHeadersAndUrl bucket remote verb = do
  -- Some things that are going to be useful throughout:
  Just awsConn <- amazonS3ConnectionFromEnv
  currentTime  <- getCurrentTime

  -- Construct a URL and the appropriate headers to use.
  let httpHost = printf "%s.s3.amazonaws.com" bucket
      Just url = URL.parse 
               $ fromString 
               $ printf "http://%s/%s" httpHost remote

  -- Sign the request
  let dateValue = formatTime timeLocale "%a, %d %b %Y %H:%M:%S GMT" currentTime
      signed = signRequest SignData
        { sdAccessKey       = awsAccessKey awsConn
        , sdSecretAccessKey = awsSecretKey awsConn
        , sdHttpVerb        = verb
        , sdContentMD5      = ""
        , sdContentType     = ""
        , sdDate            = dateValue
        , sdBucket          = bucket
        , sdResource        = remote
        }

  let headers = emptyHeaders 
                { httpDate          = Just currentTime
                , httpHost          = Just $ fromString httpHost
                , httpAuthorization = 
                    Just $ fromString 
                         $ printf "AWS %s:%s" (awsAccessKey awsConn) signed
                }

  -- Finally, construct the HTTP headers.
  return $ (url, headers)
                              
  where
    -- | Snagged from "Network.MiniHTTP.Marshal", to make sure we can
    -- | replicate the contents of the @Date@ header.
    timeLocale = TimeLocale 
                 { wDays       = [ ("Sunday", "Sun")
                                 , ("Monday", "Mon")
                                 , ("Tuesday", "Tue")
                                 , ("Wednesday", "Wed")
                                 , ("Thursday", "Thu")
                                 , ("Friday", "Fri")
                                 , ("Saturday", "Sat")
                                 ]
                 , months      = [ ("January", "Jan")
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
                 , intervals   = [ ("year", "years")
                                 , ("month", "months")
                                 , ("day", "days")
                                 , ("hour", "hours")
                                 , ("min", "mins")
                                 , ("sec", "secs")
                                 , ("usec", "usecs")
                                 ]
                 , amPm        = ("AM", "PM")
                 , dateTimeFmt = "%a %b %e %H:%M:%S %Z %Y"
                 , dateFmt     = "%m/%d/%y"
                 , timeFmt     = "%H:%M:%S"
                 , time12Fmt   = "%I:%M:%S %p"
                 }

-- | A (much) more memory efficient file uploader. This uses
-- | "Network.MiniHTTP" so that we can stream the file via a data
-- | source, avoiding using as much memory as the file is big.
hPut :: Handle -> Maybe Integer -> String -> String -> (Report -> IO()) -> IO ()
hPut handle size bucket remote report = do
  (url, headers') <- signedHeadersAndUrl bucket remote "PUT"
  let headers = headers' { httpContentLength = maybe Nothing (Just . fromInteger) size }

  -- We're finally ready, so make a connection, construct the request,
  -- and issue it.
  conn <- Client.connection url >>= Client.transport url

  let request = Request 
                PUT
                (URL.toRelative url)  -- the remote path
                1 1                   -- http 1.1
                headers

  -- The source is a handle, let hSourceUntilEOF stream it over the
  -- connection.
  source <- hSourceUntilEOF handle report

  -- Issue the actual request, and make sure we have an acceptable
  -- reply
  Client.request conn request (Just source) >>= \r ->
    case r of
      Just (reply, _) | replyStatus reply == 200 -> Connection.close conn
      Just (reply, _) -> throw $ HttpError 
                               $ printf "%d: %s" (replyStatus reply)
                                                 (replyMessage reply)
      _               -> throw $ HttpError "unknown"

  return ()

hGet handle bucket remote report = do
  (url, headers) <- signedHeadersAndUrl bucket remote "GET"

  conn <- Client.connection url >>= Client.transport url

  let request = Request
                GET
                (URL.toRelative url)
                1 1
                headers

  Client.request conn request Nothing >>= \r ->
    case r of 
      Just (reply, _) | replyStatus reply /= 200 -> 
        throw $ HttpError $ printf "%d: %s" (replyStatus reply) (replyMessage reply)
      Just (reply, Just source) ->
        let Just clh = httpContentLength $ replyHeaders reply in
        report (toInteger clh) >>= readSourceToHandle source
      _ -> throw $ HttpError "No result?"

  where
    readSourceToHandle source report = do
      result <- source
      case result of
        SourceData bs -> do
          report $ Transferred (toInteger $ B.length bs)
          B.hPut handle bs 
          readSourceToHandle source report
        SourceEOF -> report Done >> return ()
        SourceError -> throw $ HttpError "unknown"

hSourceUntilEOF :: Handle -> (Report -> IO()) -> IO Source
hSourceUntilEOF handle report = 
  return $ catch readHandle (const $ return SourceError)
  where
    readHandle = do
      eof <- hIsEOF handle
      if eof
        then report Done >> return SourceEOF
        -- XXX: this is actually wrong -- we want the real count, so
        -- delay by one? -- report only after done with the block
        else (report $ Transferred (128 * 1024)) >>
             B.hGet handle (128 * 1024) >>= return . SourceData
