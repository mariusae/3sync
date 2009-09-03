-- | 3sync provides bucket synchronization for S3.
module Main where

import           System.Environment
import           System.Console.GetOpt
import           System.IO (hFlush, stdout)
import qualified System.Console.Progress as P
import           Text.Printf
import           Data.IORef
import           Data.Time.Clock (getCurrentTime, secondsToDiffTime, UTCTime(..))
import           Data.Time.Calendar (Day(..))

import qualified Network.AWS.S3Sync as S3

data Command = Push | Pull
               deriving (Show, Eq)

data Flag = Diff
          | Help
          | Size
          | Verbose
            deriving (Show, Eq)

options =
 [ Option ['h'] [] (NoArg Help)    "show help"
 , Option ['d'] [] (NoArg Diff)    "show just the diff"
 , Option ['s'] [] (NoArg Size)    "use file size instead of MD5 for comparison"
 , Option ['v'] [] (NoArg Verbose) "verbose operations"
 ]

usage = do
  name <- getProgName
  let header = printf ("Usage: %s [OPTION...] <push|pull> <bucket> [dir]"   ++
                       "\nSet AWS_ACCESS_KEY_ID and AWS_ACCESS_KEY_SECRET " ++
                       "environment variables.") name
  return $ usageInfo header options

opts argv =
  case getOpt Permute options argv of
    (o, n, []) ->
      if Help `elem` o
        then return $ Nothing
        else do
          (cmd, (b, d)) <- parseArgs n
          return $ Just (o, cmd, b, d)
    (_, _, errs) -> usage >>= fail . (++) (concat errs)

  where
    parseArgs ("push":as) = parseBD as >>= return . (,) Push
    parseArgs ("pull":as) = parseBD as >>= return . (,) Pull
    parseArgs _           = help "You must specify a command"

    parseBD [bucket]      = return (bucket, ".")
    parseBD [bucket, dir] = return (bucket, dir)
    parseBD _             = help "You must specify a bucket"

    help = fail . flip (++) " [-h for help]"

main :: IO ()
main = do
  args <- getArgs >>= opts
  case args of 
    Just (flags, command, bucket, dir) -> execute flags command bucket dir
    Nothing -> usage >>= putStr

execute flags command b d = do
  S3.diff comparator d b >>= return . filter filter' >>= doIt
  where
    filter' op = case op of
                   S3.Push _ _ _ -> command == Push
                   S3.Pull _ _ _ -> command == Pull

    comparator
      | Size `elem` flags = S3.Size
      | otherwise         = S3.MD5
    doIt
      | Diff `elem` flags = putStr . unlines . map show
      | otherwise         = S3.execute report
    report
      -- report returns a function to use for subsequent reporting of
      -- the progress of said file.
      | Verbose `elem` flags = \op size -> do
          now <- getCurrentTime
          p   <- newIORef $ P.empty size now
          return $ \r ->
            case r of
              S3.Transferred howmany -> do
                now    <- getCurrentTime
                status <- atomicModifyIORef p $ P.drawProgressBar now . P.update howmany
                putStr $ "\r" ++ (show op) ++ ": " ++ status
                hFlush stdout
              S3.Done -> do
                putStr "\n"
                hFlush stdout
      | otherwise            = const . const . return . const $ return ()
