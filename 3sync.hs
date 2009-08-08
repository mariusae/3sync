-- | 3sync provides bucket synchronization for S3.
module Main where

import System.Environment
import System.Console.GetOpt
import Text.Printf

import qualified Network.AWS.S3Sync as S3

data Command = Push | Pull
               deriving (Show, Eq)

data Flag = Diff
          | Help
          | Size
            deriving (Show, Eq)

options =
 [ Option ['h'] [] (NoArg Help) "show help"
 , Option ['d'] [] (NoArg Diff) "show just the diff"
 , Option ['s'] [] (NoArg Size) "use file size instead of MD5 for comparison"
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

main = do
  args <- getArgs >>= opts
  case args of 
    Just (flags, command, bucket, dir) -> execute flags command bucket dir
    Nothing -> usage >>= putStr

execute flags command b d =
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

    report = putStrLn . show
