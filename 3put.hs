module Main where

import           Text.Printf (printf)
import           System.Environment (getProgName, getArgs)
import           System.IO (stdin)
import qualified Data.ByteString.Lazy as L


import           Network.AWS.AWSConnection (amazonS3ConnectionFromEnv)
import           Network.AWS.S3Object (S3Object(..), sendObject)

usage = do
  name <- getProgName
  return $ printf "Usage: %s <bucket> <path>" name

main :: IO ()
main = do
  args <- getArgs
  case args of
    [bucket, path] -> do 
      -- Unfortunately, you cannot stream objects of unknown length to
      -- S3, so we're forced to read the entirety of the input before
      -- we can send the object. This basically precludes streaming
      -- large objects :-(
      Just conn <- amazonS3ConnectionFromEnv
      contents  <- L.hGetContents stdin
      sendObject conn S3Object { obj_bucket   = bucket
                               , obj_name     = path
                               , content_type = ""
                               , obj_headers  = []
                               , obj_data     = contents
                               }
      return ()
    _ -> usage >>= fail
