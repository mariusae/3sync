module Main where

import Network.AWS.S3Sync


bucket = "tables.mixerlabs.com"

main = do
  diffs <- diff "." bucket
  print diffs