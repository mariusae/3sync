-- Provide some low-level S3 primitives.
module Network.AWS.S3 
  ( SignData(..)
  , signRequest
  ) where

import Data.HMAC (hmac_sha1)
import Data.List (intercalate)
import qualified Codec.Binary.Base64 as B64
import qualified Codec.Binary.UTF8.String as U8

-- | Require for producing signed authentication headers for S3.
data SignData =
  SignData { accessKey       :: String
           , secretAccessKey :: String
           , httpVerb        :: String
           , contentMD5      :: String
           , contentType     :: String
           , date            :: String
           , bucket          :: String
           , resource        :: String
           -- Not yet supported: AWS headers
           }

-- | Sign a request as per the given @SignData@, returning a base-64
-- | encoded string appropriate for use in Amazon S3 authorization
-- | headers.
signRequest :: SignData -> String
signRequest (SignData ak sak hv cmd5 ct d b r) =
  sign $ U8.encode $ intercalate "\n" [hv, cmd5, ct, d, "/" ++ b ++ "/" ++ r]
  where
    sign = B64.encode . hmac_sha1 (U8.encode sak)

